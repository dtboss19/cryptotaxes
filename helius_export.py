#!/usr/bin/env python3
import os
import sys
import json
import csv
import time
from typing import Any, Dict, List, Optional, Set, Tuple
from datetime import datetime

import requests
from dateutil import parser as dateparser
from tqdm import tqdm

HELIUS_BASE = "https://api.helius.xyz/v0"

# Common Bubblegum program ids (mainnet variants observed)
BUBBLEGUM_PROGRAM_IDS: Set[str] = {
	"BGUMApV3npVqfY3VhXv9Gqz3r3Gq5h5xQmYkYw2nVBoz",  # placeholder variant
	"BGUMAp7x2hAqHcC1EHnHCqB6fN5teLo75fW4rWuBbY",    # placeholder variant
}


def load_wallets(path: str) -> List[str]:
	with open(path, "r", encoding="utf-8") as f:
		wallets = json.load(f)
	if not isinstance(wallets, list) or not all(isinstance(x, str) for x in wallets):
		raise ValueError("wallets.json must be a JSON array of base58 wallet strings")
	return wallets


def iso_to_unix_ms(iso_time: str) -> int:
	return int(dateparser.isoparse(iso_time).timestamp() * 1000)


def unix_ms_to_iso(ms: int) -> str:
	return datetime.utcfromtimestamp(ms / 1000).isoformat() + "Z"


def request_with_retries(url: str, method: str = "GET", params: Optional[Dict[str, Any]] = None, json_body: Optional[Dict[str, Any]] = None, timeout: int = 30, max_retries: int = 5, backoff: float = 0.8) -> requests.Response:
	for attempt in range(max_retries):
		try:
			if method.upper() == "GET":
				resp = requests.get(url, params=params, timeout=timeout)
			else:
				resp = requests.post(url, params=params, json=json_body, timeout=timeout)
			if resp.status_code >= 500:
				raise requests.HTTPError(f"Server error {resp.status_code}")
			return resp
		except (requests.ConnectionError, requests.Timeout, requests.HTTPError):
			sleep_s = (backoff ** attempt) * 1.0
			time.sleep(min(10.0, sleep_s))
	raise RuntimeError("Exhausted retries")


def fetch_enriched_transactions(api_key: str, wallet: str, limit: int = 1000, start_time_ms: Optional[int] = None, end_time_ms: Optional[int] = None) -> List[Dict[str, Any]]:
	"""Fetch enriched transactions for a single wallet with pagination.
	Helius GET /v0/addresses/{address}/transactions does not accept a 'limit' query param here,
	so we only use 'before' and stop locally when reaching 'limit'.
	"""
	transactions: List[Dict[str, Any]] = []
	before: Optional[str] = None  # signature to paginate older
	fetched = 0
	while True:
		if fetched >= limit:
			break
		params: Dict[str, Any] = {"api-key": api_key}
		if before:
			params["before"] = before
		if start_time_ms is not None:
			params["startTime"] = start_time_ms
		if end_time_ms is not None:
			params["endTime"] = end_time_ms
		url = f"{HELIUS_BASE}/addresses/{wallet}/transactions"
		resp = request_with_retries(url, params=params)
		if resp.status_code != 200:
			raise RuntimeError(f"Helius error {resp.status_code}: {resp.text}")
		batch = resp.json()
		if not isinstance(batch, list) or len(batch) == 0:
			break
		transactions.extend(batch)
		fetched += len(batch)
		before = batch[-1].get("signature")
		if not before:
			break
	return transactions[:limit]


def is_self_transfer(our_wallets: Set[str], tx: Dict[str, Any]) -> bool:
	addrs: Set[str] = set()
	for nt in tx.get("nativeTransfers", []) or []:
		if nt.get("fromUserAccount"):
			addrs.add(nt["fromUserAccount"])
		if nt.get("toUserAccount"):
			addrs.add(nt["toUserAccount"])
	for tt in tx.get("tokenTransfers", []) or []:
		if tt.get("fromUserAccount"):
			addrs.add(tt["fromUserAccount"])
		if tt.get("toUserAccount"):
			addrs.add(tt["toUserAccount"])
	inter = addrs.intersection(our_wallets)
	return len(inter) >= 2


def sum_amounts_relative_to_wallets(our_wallets: Set[str], tx: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], int]:
	"""Return list of per-asset movements and fee in lamports (SOL).
	Each movement includes asset, amount, decimals, mint, from_user, to_user.
	Positive amount means incoming to our wallets; negative is outgoing.
	"""
	movements: List[Dict[str, Any]] = []
	# Native SOL
	for nt in tx.get("nativeTransfers", []) or []:
		lamports = int(nt.get("amount", 0))
		from_acct = nt.get("fromUserAccount")
		to_acct = nt.get("toUserAccount")
		if to_acct in our_wallets and from_acct not in our_wallets:
			movements.append({
				"asset": "SOL", "mint": None, "decimals": 9,
				"amount": lamports / 1e9,
				"from_user": from_acct, "to_user": to_acct,
			})
		elif from_acct in our_wallets and to_acct not in our_wallets:
			movements.append({
				"asset": "SOL", "mint": None, "decimals": 9,
				"amount": -lamports / 1e9,
				"from_user": from_acct, "to_user": to_acct,
			})
	# Tokens
	for tt in tx.get("tokenTransfers", []) or []:
		amt_raw = int(tt.get("tokenAmount", 0))
		dec = int(tt.get("tokenDecimals", 0) or 0)
		mint = tt.get("mint")
		sym = (tt.get("tokenSymbol") or mint or "TOKEN").upper()
		from_acct = tt.get("fromUserAccount")
		to_acct = tt.get("toUserAccount")
		amount_adj = amt_raw / (10 ** dec if dec else 1)
		if to_acct in our_wallets and from_acct not in our_wallets:
			movements.append({
				"asset": sym, "mint": mint, "decimals": dec,
				"amount": amount_adj,
				"from_user": from_acct, "to_user": to_acct,
			})
		elif from_acct in our_wallets and to_acct not in our_wallets:
			movements.append({
				"asset": sym, "mint": mint, "decimals": dec,
				"amount": -amount_adj,
				"from_user": from_acct, "to_user": to_acct,
			})
	fee_lamports = int((tx.get("fee") or 0))
	return movements, fee_lamports


def derive_transaction_type(tx: Dict[str, Any], is_self: bool, movements: List[Dict[str, Any]], spam_flag: bool) -> str:
	if spam_flag:
		return "spam_cnft"
	if is_self:
		return "transfer_internal"
	category = (tx.get("type") or "").lower()
	source = (tx.get("source") or "").lower()
	if "swap" in source or category == "swap":
		return "trade"
	if category in {"nft", "nft_sale", "nft_mint"} or "nft" in source:
		return "nft"
	if "stake" in source or category in {"stake", "unstake"}:
		return "staking"
	net = sum(m.get("amount", 0.0) for m in movements if isinstance(m.get("amount"), (int, float)))
	if net > 0:
		return "income"
	if net < 0:
		return "spend"
	return "transfer"


def get_primary_program_id(tx: Dict[str, Any]) -> str:
	pid = tx.get("programId") or ""
	if pid:
		return pid
	# try instructions list
	instrs = tx.get("instructions") or []
	if isinstance(instrs, list) and instrs:
		pi = instrs[0].get("programId") if isinstance(instrs[0], dict) else None
		if isinstance(pi, str):
			return pi
	return ""


def is_bubblegum_spam(tx: Dict[str, Any], movements: List[Dict[str, Any]]) -> bool:
	source = (tx.get("source") or "").lower()
	pid = get_primary_program_id(tx)
	is_bg = ("bubblegum" in source) or (pid in BUBBLEGUM_PROGRAM_IDS)
	if not is_bg:
		return False
	# near-zero SOL net implies likely airdropped cNFT attach
	sol_net = sum(m.get("amount", 0.0) for m in movements if m.get("asset") == "SOL")
	return abs(sol_net) <= 0.00001


def write_csv(rows: List[Dict[str, Any]], output_path: str) -> None:
	fieldnames = [
		"timestamp",
		"txid",
		"program_id",
		"program_source",
		"helius_type",
		"derived_type",
		"asset",
		"amount",
		"fee_sol",
		"is_self_transfer",
		"spam_flag",
		"from",
		"to",
		"description",
		"cost_basis_usd",
	]
	with open(output_path, "w", newline="", encoding="utf-8") as f:
		writer = csv.DictWriter(f, fieldnames=fieldnames)
		writer.writeheader()
		for r in rows:
			writer.writerow(r)


def build_rows_for_wallet(api_key: str, wallet: str, our_wallets: Set[str], start_ms: Optional[int], end_ms: Optional[int], limit: int) -> List[Dict[str, Any]]:
	rows: List[Dict[str, Any]] = []
	transactions = fetch_enriched_transactions(api_key, wallet, limit=limit, start_time_ms=start_ms, end_time_ms=end_ms)
	for tx in transactions:
		movements, fee_lamports = sum_amounts_relative_to_wallets(our_wallets, tx)
		is_self = is_self_transfer(our_wallets, tx)
		spam_flag = is_bubblegum_spam(tx, movements)
		dtype = derive_transaction_type(tx, is_self, movements, spam_flag)
		timestamp_ms = int(tx.get("timestamp", 0)) * 1000 if tx.get("timestamp") and tx.get("timestamp") < 10**12 else int(tx.get("timestamp", 0))
		iso_ts = unix_ms_to_iso(timestamp_ms) if timestamp_ms else ""
		program_id = get_primary_program_id(tx)
		source = tx.get("source") or ""
		helius_type = tx.get("type") or ""
		if not movements:
			rows.append({
				"timestamp": iso_ts,
				"txid": tx.get("signature", ""),
				"program_id": program_id,
				"program_source": source,
				"helius_type": helius_type,
				"derived_type": dtype,
				"asset": "",
				"amount": 0,
				"fee_sol": fee_lamports / 1e9 if fee_lamports else 0,
				"is_self_transfer": str(is_self).lower(),
				"spam_flag": str(spam_flag).lower(),
				"from": "",
				"to": "",
				"description": f"program={source} type={helius_type}",
				"cost_basis_usd": "",
			})
			continue
		for m in movements:
			rows.append({
				"timestamp": iso_ts,
				"txid": tx.get("signature", ""),
				"program_id": program_id,
				"program_source": source,
				"helius_type": helius_type,
				"derived_type": dtype,
				"asset": m.get("asset", ""),
				"amount": m.get("amount", 0),
				"fee_sol": fee_lamports / 1e9 if fee_lamports else 0,
				"is_self_transfer": str(is_self).lower(),
				"spam_flag": str(spam_flag).lower(),
				"from": m.get("from_user", ""),
				"to": m.get("to_user", ""),
				"description": f"program={source} type={helius_type} mint={m.get('mint')}",
				"cost_basis_usd": "",
			})
	return rows


def main(argv: List[str]) -> int:
	import argparse
	parser = argparse.ArgumentParser(description="Export Solana transactions to CSV using Helius enriched API")
	parser.add_argument("--api-key", dest="api_key", default=os.environ.get("HELIUS_API_KEY"), help="Helius API key")
	parser.add_argument("--wallets", dest="wallets_path", default="wallets.json", help="Path to wallets.json list")
	parser.add_argument("--output", dest="output_path", default="output.csv", help="CSV output path")
	parser.add_argument("--start", dest="start", default=None, help="Start time ISO (inclusive)")
	parser.add_argument("--end", dest="end", default=None, help="End time ISO (exclusive)")
	parser.add_argument("--limit", dest="limit", type=int, default=1000, help="Max transactions per wallet to fetch")
	args = parser.parse_args(argv)

	if not args.api_key:
		raise SystemExit("Missing Helius API key. Provide --api-key or set HELIUS_API_KEY.")

	wallets = load_wallets(args.wallets_path)
	our_wallets: Set[str] = set(x.strip() for x in wallets)
	start_ms = iso_to_unix_ms(args.start) if args.start else None
	end_ms = iso_to_unix_ms(args.end) if args.end else None

	all_rows: List[Dict[str, Any]] = []
	for w in tqdm(wallets, desc="Wallets"):
		rows = build_rows_for_wallet(args.api_key, w, our_wallets, start_ms, end_ms, limit=args.limit)
		all_rows.extend(rows)

	write_csv(all_rows, args.output_path)
	print(f"Wrote {len(all_rows)} rows to {args.output_path}")
	return 0


if __name__ == "__main__":
	sys.exit(main(sys.argv[1:]))
