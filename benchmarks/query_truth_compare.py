"""Compare query-truth JSONL files. Output a JSON mismatch report to stdout."""

import argparse
import json
import math
import sys


def load_records(path):
    records = {}
    duplicates = []
    with open(path, encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, 1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
                if not isinstance(record, dict):
                    raise ValueError("record must be an object")
                symbol, window, kind = (record[field] for field in ("symbol", "window", "type"))
                if not isinstance(symbol, str) or not symbol or type(window) is not int:
                    raise ValueError("symbol must be a nonempty string and window an integer")
                if kind not in ("EMA", "BUY", "SELL"):
                    raise ValueError("type must be EMA, BUY, or SELL")
                if kind == "EMA":
                    for field in ("ema38", "ema100"):
                        value = record[field]
                        if type(value) not in (int, float) or not math.isfinite(value):
                            raise ValueError(f"{field} must be a finite number")
                key = (symbol, window, kind)
                if key in records:
                    duplicates.append(dict(symbol=symbol, window=window, type=kind))
                else:
                    records[key] = record
            except (ValueError, KeyError, TypeError, OverflowError) as exc:
                raise ValueError(f"{path}:{line_number}: {exc}") from exc
    return records, duplicates


def compare(expected, actual):
    expected_records, expected_duplicates = load_records(expected)
    actual_records, actual_duplicates = load_records(actual)
    expected_keys, actual_keys = set(expected_records), set(actual_records)

    def key_record(key):
        return dict(zip(("symbol", "window", "type"), key))

    missing = [key_record(key) for key in sorted(expected_keys - actual_keys)]
    extra = [key_record(key) for key in sorted(actual_keys - expected_keys)]
    incorrect = []
    for key in sorted(expected_keys & actual_keys):
        if key[2] == "EMA":
            for field in ("ema38", "ema100"):
                left, right = expected_records[key][field], actual_records[key][field]
                if not math.isclose(left, right, rel_tol=1e-9, abs_tol=1e-9):
                    incorrect.append(key_record(key) | {"field": field, "expected": left, "actual": right})
    duplicates = {"expected": expected_duplicates, "actual": actual_duplicates}
    return {"match": not (missing or extra or incorrect or expected_duplicates or actual_duplicates),
            "missing": missing, "extra": extra, "duplicates": duplicates, "incorrect": incorrect}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("expected", help="expected JSONL file")
    parser.add_argument("actual", help="actual JSONL file")
    args = parser.parse_args()
    try:
        report = compare(args.expected, args.actual)
    except (OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(report, allow_nan=False))
    return 0 if report["match"] else 1


if __name__ == "__main__":
    sys.exit(main())
