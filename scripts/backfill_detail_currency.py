"""
One-time (re-runnable) backfill of the "currency" field in
docs/data/details/{TICKER}.json.

Historic builds defaulted currency to "USD" (or left it null) even for
international listings whose prices are stored in their native currency
(e.g. SK hynix 000660 priced in KRW). This script repairs that using the
exchange-suffix inference from predator.fetch_stock_details:

  - inferred = infer_currency_from_symbol(symbol_map.get(ticker, ticker))
  - inferred is not None, file currency in {null, "", "USD"} and != inferred
                                                        → set inferred
  - inferred is None and file currency is null          → set "USD"
  - otherwise                                           → leave file untouched

A non-USD/non-empty currency in the file came from yfinance info and always
wins over suffix inference (info > inference priority); it is never
overwritten.

Files are only rewritten when the currency actually changes (preserves
mtimes, keeps the git diff minimal). Writes are atomic.

Usage:  python scripts/backfill_detail_currency.py
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from predator.fetch_stock_details import infer_currency_from_symbol  # noqa: E402

DETAILS_DIR     = REPO_ROOT / "docs" / "data" / "details"
SYMBOL_MAP_PATH = REPO_ROOT / "data" / "symbol_map.json"


def _write_json_atomic(path: Path, payload: dict) -> None:
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def main() -> None:
    symbol_map: dict[str, str] = json.loads(SYMBOL_MAP_PATH.read_text(encoding="utf-8"))

    fixed_from_usd = 0
    fixed_from_null = 0
    defaulted_usd = 0
    untouched = 0
    skipped_pending = 0
    errors = 0

    for path in sorted(DETAILS_DIR.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"  WARNING: could not parse {path.name}: {e}")
            errors += 1
            continue

        if payload.get("pending"):
            # Stubs carry no price data — currency gets set on first real write.
            skipped_pending += 1
            continue

        ticker = payload.get("ticker") or path.stem
        current = payload.get("currency")
        inferred = infer_currency_from_symbol(symbol_map.get(ticker, ticker))

        if inferred is not None and current in (None, "", "USD") and current != inferred:
            old = current
            payload["currency"] = inferred
            _write_json_atomic(path, payload)
            if old is None:
                fixed_from_null += 1
            else:
                fixed_from_usd += 1
        elif inferred is None and current is None:
            payload["currency"] = "USD"
            _write_json_atomic(path, payload)
            defaulted_usd += 1
        else:
            untouched += 1

    print("Backfill complete:")
    print(f"  fixed-from-USD : {fixed_from_usd}")
    print(f"  fixed-from-null: {fixed_from_null}")
    print(f"  defaulted-USD  : {defaulted_usd}")
    print(f"  untouched      : {untouched}")
    print(f"  pending-skipped: {skipped_pending}")
    if errors:
        print(f"  parse-errors   : {errors}")


if __name__ == "__main__":
    main()
