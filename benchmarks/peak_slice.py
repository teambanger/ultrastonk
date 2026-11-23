#!/usr/bin/env python3
"""Carve a replayable peak slice out of the official DEBS CSV for ticket 07.

The slice is defined by ticket 02's verified fixture (`week-fixture.json`): the
highest rolling five-minute raw-row capture interval, its exchange mix, and the
pre-roll/trailing context available around it. This tool copies the selected
rows in original physical (source) order so burst order is preserved, and it
labels a pre-roll, measured and trailing segment exactly the way the capture
paced emitter does.

Selection mirrors `scripts/profile_fixture.py`: 40-column rows whose last-dot
suffix is FR, NL or ETR and whose system capture time (columns 2/3) parses and
falls in the requested interval. Capture time is interpreted on the same naive
UTC basis as the fixture, so the row membership and exchange mix reproduce the
fixture exactly. The emitter interprets capture time in its configured event
zone, but only differences matter for segment labels, so the offset is
invariant.

Raw CSVs stay outside Git. Run `python3 benchmarks/peak_slice.py --help`.
"""
import argparse
import csv
import hashlib
import io
import json
import os
import re
import sys
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path

SKIP_LINES = 13
COLUMNS = 40
EXCHANGES = ("FR", "NL", "ETR")
EPOCH = datetime(1970, 1, 1)
DATE_RE = re.compile(r"\d{2}-\d{2}-\d{4}")
TIME_RE = re.compile(r"\d{2}:\d{2}:\d{2}(?:\.\d{0,9})?")

SEGMENTS = ("PRE_ROLL", "MEASURED", "TRAILING")

# Documented defaults. Pre-roll covers one full five-minute window so at least
# one complete preceding window finalises before the measured peak and the EMA
# state entering the peak is non-zero. Trailing extends past the close of the
# last window the measured segment touches (measured end rounds up to the next
# boundary at +5:00, plus the disorder budget, plus margin) so the watermark
# advances and that window finalises naturally instead of only at end of input.
DEFAULT_PRE_ROLL_MS = 300_000
DEFAULT_TRAILING_MS = 180_000


def capture_ms(date, time):
    """Parse a DEBS system date/time pair to epoch milliseconds on a naive UTC basis.

    Mirrors `scripts/profile_week.capture_ns`, truncated to milliseconds. Because
    the fixture's peak boundaries are exact millisecond instants, millisecond
    truncation selects the same rows as the fixture's nanosecond comparison.
    """
    if not DATE_RE.fullmatch(date) or not TIME_RE.fullmatch(time):
        raise ValueError("invalid date or time format")
    whole, _, fraction = time.partition(".")
    if len(fraction) > 9 or (fraction and not fraction.isdigit()):
        raise ValueError("invalid fractional seconds")
    moment = datetime.strptime(date + " " + whole, "%d-%m-%Y %H:%M:%S")
    seconds = (moment - EPOCH) // timedelta(seconds=1)
    return seconds * 1000 + int((fraction + "000000000")[:3])


def exchange_of(symbol):
    """Return the exchange suffix after the last dot, or None when there is none."""
    base, separator, exchange = symbol.rpartition(".")
    return exchange if separator and base else None


def parse_iso_ms(value):
    """Parse the fixture's ISO-8601 nanosecond stamp to epoch milliseconds (naive UTC)."""
    date, _, time = value.partition("T")
    return capture_ms(datetime.strptime(date, "%Y-%m-%d").strftime("%d-%m-%Y"), time)


def load_fixture(path):
    """Load the fixture and return (fixture, measured_start_ms, measured_end_ms)."""
    fixture = json.loads(Path(path).read_text())
    peak = fixture.get("raw_peak")
    if not isinstance(peak, dict) or "start" not in peak or "end_exclusive" not in peak:
        raise ValueError("fixture has no raw_peak start/end_exclusive")
    start = parse_iso_ms(peak["start"])
    end = parse_iso_ms(peak["end_exclusive"])
    if end <= start:
        raise ValueError("fixture peak end must be after start")
    return fixture, start, end


def slice_window(measured_start_ms, measured_end_ms, pre_roll_ms, trailing_ms, state="warm"):
    """Return the half-open [slice_start, slice_end) capture interval for a run state.

    `state="cold"` drops the pre-roll so the measured peak is offered with the
    EMA state starting at zero; `state="warm"` prepends the requested pre-roll.
    """
    if measured_end_ms <= measured_start_ms:
        raise ValueError("measured end must be after measured start")
    if pre_roll_ms < 0 or trailing_ms < 0:
        raise ValueError("pre-roll and trailing must be nonnegative")
    if state not in ("warm", "cold"):
        raise ValueError("state must be warm or cold")
    pre = 0 if state == "cold" else int(pre_roll_ms)
    return measured_start_ms - pre, measured_end_ms + int(trailing_ms)


def _capture_stamp(row):
    if len(row) != COLUMNS:
        return None
    if not row[0].strip() or not row[1].strip():
        return None
    if exchange_of(row[0].strip()) not in EXCHANGES:
        return None
    try:
        return capture_ms(row[2].strip(), row[3].strip())
    except ValueError:
        return None


def emitter_offsets(anchor_ms, measured_start_ms, measured_end_ms):
    """Return (preroll_ms, measure_ms) as the capture emitter must be told.

    `anchor_ms` is the capture time of the first row physically offered to the
    emitter, which is the emitter's own offset reference. When a pre-roll is
    present the offsets are measured from the measured interval; when the first
    offered row is already past the measured start (cold start, or a boundary
    gap) the measured segment is taken from that first row instead.
    """
    if anchor_ms <= measured_start_ms:
        return measured_start_ms - anchor_ms, measured_end_ms - measured_start_ms
    return 0, measured_end_ms - anchor_ms


def label_segment(stamp_ms, anchor_ms, preroll_ms, measure_ms):
    """Mirror ReplayPacer.segmentFor for one capture stamp."""
    offset = stamp_ms - anchor_ms
    if offset < preroll_ms:
        return "PRE_ROLL"
    if measure_ms > 0 and offset >= preroll_ms + measure_ms:
        return "TRAILING"
    return "MEASURED"


def extract_slice(source, output, measured_start_ms, measured_end_ms,
                  pre_roll_ms=DEFAULT_PRE_ROLL_MS, trailing_ms=DEFAULT_TRAILING_MS,
                  state="warm", reorder="physical"):
    """Copy the in-range rows and return a slice manifest.

    The output CSV keeps the 13 header lines so the replay emitter can read it
    with its normal loader. Row membership is fixed by the capture interval and
    the loader's structural screen.

    `reorder="physical"` preserves original source order (the recorded burst
    order). `reorder="capture"` writes the fixture's documented timestamp order
    instead: ascending system capture time, then original source ordinal for
    ties. Capture mode is required when the first physical row of the source
    starts after the measured start, because the capture pacer anchors its
    segment offsets on the first offered row; physical order would otherwise
    label most of the measured window as pre-roll.
    """
    if reorder not in ("physical", "capture"):
        raise ValueError("reorder must be physical or capture")
    source = Path(source)
    if not source.is_file():
        raise ValueError(f"CSV file is not readable: {source}")
    slice_start, slice_end = slice_window(measured_start_ms, measured_end_ms,
                                          pre_roll_ms, trailing_ms, state)
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(output.name + ".tmp")

    segment_rows = Counter()
    measured_by_exchange = Counter()
    all_by_exchange = Counter()
    anchor_ms = None
    first_measured_ms = last_measured_ms = None
    copied = 0
    digest = hashlib.sha256()
    entries = [] if reorder == "capture" else None
    buffer = io.StringIO() if reorder == "capture" else None
    try:
        with source.open(newline="", encoding="utf-8") as stream, \
                temporary.open("w", newline="", encoding="utf-8") as target:
            header = [next(stream, "") for _ in range(SKIP_LINES)]
            if not all(header):
                raise ValueError("source has fewer than 13 header lines")
            target.write("".join(header))
            if reorder == "physical":
                digest.update("".join(header).encode())
            writer = csv.writer(target)
            ordinal = 0
            for row in csv.reader(stream):
                stamp = _capture_stamp(row)
                if stamp is None or not (slice_start <= stamp < slice_end):
                    continue
                if reorder == "capture":
                    buffer.seek(0)
                    buffer.truncate(0)
                    csv.writer(buffer).writerow(row)
                    entries.append((stamp, ordinal, buffer.getvalue()))
                else:
                    writer.writerow(row)
                    digest.update((",".join(row) + "\n").encode())
                    if anchor_ms is None:
                        anchor_ms = stamp
                ordinal += 1
                copied += 1
                exchange = exchange_of(row[0].strip())
                all_by_exchange[exchange] += 1
                if stamp < measured_start_ms:
                    segment_rows["PRE_ROLL"] += 1
                elif stamp < measured_end_ms:
                    segment_rows["MEASURED"] += 1
                    measured_by_exchange[exchange] += 1
                    first_measured_ms = stamp if first_measured_ms is None else min(first_measured_ms, stamp)
                    last_measured_ms = stamp if last_measured_ms is None else max(last_measured_ms, stamp)
                else:
                    segment_rows["TRAILING"] += 1
            if reorder == "capture" and entries:
                entries.sort(key=lambda item: (item[0], item[1]))
                anchor_ms = entries[0][0]
                digest.update("".join(header).encode())
                for _, _, text in entries:
                    target.write(text)
                    digest.update(text.encode())
        if anchor_ms is None:
            raise ValueError("no row fell in the requested capture interval")
        if segment_rows["MEASURED"] == 0:
            raise ValueError("the measured segment is empty")
        os.replace(temporary, output)
    finally:
        if temporary.exists():
            temporary.unlink()

    preroll_ms, measure_ms = emitter_offsets(anchor_ms, measured_start_ms, measured_end_ms)
    # The emitter must reproduce the measured membership we counted here.
    labeled_measured = segment_rows["MEASURED"]
    if label_segment(measured_start_ms, anchor_ms, preroll_ms, measure_ms) != "MEASURED":
        raise ValueError("emitter offsets do not place the measured start in the measured segment")
    return {
        "source": str(source),
        "output": str(output),
        "state": state,
        "rows": copied,
        "bytes": output.stat().st_size,
        "sha256": digest.hexdigest(),
        "slice_start_ms": slice_start,
        "slice_end_ms": slice_end,
        "measured_start_ms": measured_start_ms,
        "measured_end_ms": measured_end_ms,
        "first_capture_ms": anchor_ms,
        "first_measured_ms": first_measured_ms,
        "last_measured_ms": last_measured_ms,
        "pre_roll_requested_ms": 0 if state == "cold" else pre_roll_ms,
        "trailing_requested_ms": trailing_ms,
        "segments": {segment: segment_rows[segment] for segment in SEGMENTS},
        "measured_by_exchange": {exchange: measured_by_exchange[exchange] for exchange in EXCHANGES},
        "all_by_exchange": {exchange: all_by_exchange[exchange] for exchange in EXCHANGES},
        "emitter": {"PACING": "capture", "REPLAY_PREROLL_MS": preroll_ms,
                    "REPLAY_MEASURE_MS": measure_ms},
        "labeled_measured": labeled_measured,
    }


def verify_against_fixture(manifest, fixture):
    """Return a verdict comparing a slice manifest to the fixture's headline counts."""
    peak = fixture["raw_peak"]
    expected_rows = peak["rows"]
    expected_exchange = fixture.get("counts_by_exchange") or {}
    problems = []
    if manifest["segments"]["MEASURED"] != expected_rows:
        problems.append(f"measured rows {manifest['segments']['MEASURED']} != fixture {expected_rows}")
    for exchange in EXCHANGES:
        expected = expected_exchange.get(exchange)
        actual = manifest["measured_by_exchange"][exchange]
        if expected is not None and actual != expected:
            problems.append(f"measured {exchange} {actual} != fixture {expected}")
    if manifest["first_measured_ms"] != parse_iso_ms(peak["start"]):
        problems.append("first measured capture time does not match the fixture peak start")
    latest = (fixture.get("latest") or {}).get("timestamp")
    if latest is not None and manifest["last_measured_ms"] != parse_iso_ms(latest):
        problems.append("last measured capture time does not match the fixture peak latest")
    return {"verified": not problems, "problems": problems,
            "expected_rows": expected_rows, "measured_rows": manifest["segments"]["MEASURED"]}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path, required=True, help="ticket 02 week-fixture.json")
    parser.add_argument("--data-dir", type=Path, required=True, help="directory holding the official CSVs")
    parser.add_argument("--output", type=Path, required=True, help="slice CSV to write")
    parser.add_argument("--manifest-out", type=Path, help="write the slice manifest JSON here")
    parser.add_argument("--state", choices=("warm", "cold"), default="warm")
    parser.add_argument("--pre-roll-ms", type=int, default=DEFAULT_PRE_ROLL_MS)
    parser.add_argument("--trailing-ms", type=int, default=DEFAULT_TRAILING_MS)
    parser.add_argument("--reorder", choices=("physical", "capture"), default="physical",
                        help="row order: original source order, or capture-time order with source-ordinal ties")
    parser.add_argument("--skip-verify", action="store_true",
                        help="do not fail when measured counts differ from the fixture")
    args = parser.parse_args(argv)
    fixture, start, end = load_fixture(args.fixture)
    source_name = (fixture.get("earliest") or {}).get("source", {}).get("file")
    if not source_name:
        parser.error("fixture does not name a source file")
    source = args.data_dir / source_name
    manifest = extract_slice(source, args.output, start, end, args.pre_roll_ms,
                             args.trailing_ms, args.state, args.reorder)
    verdict = verify_against_fixture(manifest, fixture)
    manifest["fixture"] = {"path": str(args.fixture), "verified": verdict["verified"],
                           "problems": verdict["problems"], "expected_rows": verdict["expected_rows"]}
    report = {"slice": manifest, "fixture_verification": verdict}
    if args.manifest_out:
        args.manifest_out.parent.mkdir(parents=True, exist_ok=True)
        args.manifest_out.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))
    if not verdict["verified"] and not args.skip_verify:
        print("error: slice does not reproduce the fixture headline counts", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
