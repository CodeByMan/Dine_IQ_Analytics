from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from dineiq.config import load_settings
from dineiq.dataset.schema import DATE_FIELDS, TIMESTAMP_FIELDS, DECIMAL_FIELDS, INTEGER_FIELDS, BOOLEAN_FIELDS


def canonical(frame):
    result = frame.copy()
    for name in result:
        value = result[name]
        blank = value.isna() | value.astype(str).eq("")
        if name in DATE_FIELDS | TIMESTAMP_FIELDS:
            parsed = pd.to_datetime(value.where(~blank), errors="raise", format="mixed")
            value = parsed.dt.strftime("%Y-%m-%d %H:%M:%S" if name in TIMESTAMP_FIELDS else "%Y-%m-%d")
        elif name in DECIMAL_FIELDS | INTEGER_FIELDS:
            number = pd.to_numeric(value.where(~blank), errors="raise")
            precision = 2 if name in DECIMAL_FIELDS else 0
            value = number.map(lambda v: "" if pd.isna(v) else format(v, f".{precision}f"))
        elif name in BOOLEAN_FIELDS:
            value = value.astype(str).str.lower().replace({"1": "true", "1.0": "true", "0": "false", "0.0": "false"})
        result[name] = value.where(~blank, "").fillna("").astype(str)
    return result


def fingerprint(batches):
    rows = 0
    sums = {}
    for frame in batches:
        normalized = canonical(frame)
        rows += len(frame)
        for column in normalized:
            # Two order-independent modular sums detect missing/changed values
            # without assuming CSV and Parquet batching/row order are identical.
            hashes = pd.util.hash_pandas_object(normalized[column], index=False).to_numpy()
            a, b = sums.get(column, (0, 0))
            sums[column] = ((a + sum(map(int, hashes))) % (1 << 64),
                            (b + sum(int(v) ** 2 for v in hashes)) % (1 << 64))
        # Retain cross-column correlation: per-column parity alone is insufficient.
        hashes = pd.util.hash_pandas_object(normalized, index=False).to_numpy()
        a, b = sums.get("__row_hash__", (0, 0))
        sums["__row_hash__"] = ((a + sum(map(int, hashes))) % (1 << 64),
                                (b + sum(int(v) ** 2 for v in hashes)) % (1 << 64))
    return rows, sums


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="Fresh JSON report path")
    args = parser.parse_args()
    settings = load_settings()
    raw = settings.raw_data_dir.resolve()
    if args.output.resolve() == raw or raw in args.output.resolve().parents:
        raise ValueError("Reports must not be written into raw_data")
    if args.output.exists():
        raise FileExistsError("Existing verification evidence is never overwritten")
    try:
        import pyarrow.parquet as pq
    except ImportError:
        print("PARQUET_STATUS=BLOCKED")
        return 2
    sources = [(p, settings.parquet_dir / "tables" / (p.stem + ".parquet"))
               for p in sorted(settings.processed_data_dir.glob("*.csv"))]
    sources += [(p, settings.parquet_dir / "splits" / p.parent.name / (p.stem + ".parquet"))
                for split in ("train", "validation", "test")
                for p in sorted((settings.splits_dir / split).glob("*.csv"))]
    results = []
    for csv_path, parquet_path in sources:
        row = {"csv": str(csv_path), "parquet": str(parquet_path), "passed": False}
        try:
            parquet = pq.ParquetFile(parquet_path)
            with pd.read_csv(csv_path, dtype=str, keep_default_na=False, chunksize=100000) as reader:
                csv_rows, csv_hash = fingerprint(reader)
            parquet_rows, parquet_hash = fingerprint(batch.to_pandas() for batch in parquet.iter_batches(batch_size=100000))
            row.update({"csv_rows": csv_rows, "parquet_rows": parquet_rows,
                        "arrow_schema": str(parquet.schema_arrow), "bytes": parquet_path.stat().st_size,
                        "canonical_value_parity": csv_hash == parquet_hash,
                        "passed": csv_rows == parquet_rows and csv_hash == parquet_hash})
        except Exception as exc:
            row["error"] = f"{type(exc).__name__}: {exc}"
        results.append(row)
    passed = bool(results) and all(r["passed"] for r in results)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump({"results": results, "passed": passed,
                   "scope": "Cleaned tables and transaction splits; column types reported, canonical row/value fingerprints checked",
                   "limitation": "Fingerprints are probabilistic parity checks; they do not establish business correctness or Spark consumption"}, stream, indent=2)
    print(f"PARQUET_FILES_CHECKED={len(results)}")
    print(f"PARQUET_FAILURES={sum(not r['passed'] for r in results)}")
    print("PARQUET_STATUS=" + ("PASS" if passed else "FAIL"))
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
