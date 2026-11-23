# Complete-input query truth

This is an independent batch interpretation, not a statement that the current live pipeline produces these outputs. It reads raw DEBS CSV directly, without the emitter parser or Flink. Python 3.9+ and the system IANA timezone database are required.

```
python3 benchmarks/query_truth.py --timezone UTC benchmarks/fixtures/query-truth.csv
python3 -m unittest discover -s benchmarks/tests -p 'test_query_truth*.py' -v
```

Output is JSON Lines: an `EMA` record per nonempty symbol window and a `BUY` or `SELL` record only when triggered. `window` is the epoch-millisecond inclusive start; `timestamp_ms` is the closing trade time. Supply multiple raw CSV paths in original source order; 13 leading lines are skipped in each (override with `--skip-lines`). Intermediate closes are stored in a temporary SQLite database, so input aggregation is disk-backed. The temporary filesystem needs sufficient space. Choose `--timezone Europe/Berlin` to match the emitter's default, or explicitly use the run's configured `EVENT_TIME_ZONE`.

For comparison, normalize observed results into the same JSONL schema first, then run `python3 benchmarks/query_truth_compare.py expected.jsonl actual.jsonl`. Exit 0 means exact keys and direction and EMA values within both `abs_tol=1e-9` and `rel_tol=1e-9` as in `math.isclose`; exit 1 reports missing, extra, duplicate or incorrect results; exit 2 denotes invalid input. The comparator ignores closing timestamps and retains its inputs in memory. It does not itself consume Kafka or translate production records. Compare only equivalent scopes and configurations.

## Reference interpretation

- A row must have 40 columns, a nonblank security type, an exchange-qualified identifier of the form `STOCK.EXCHANGE`, and a finite strictly positive `last` price at column 21 (zero-based). Security type is not a trade filter. Quotes, invalid prices and malformed rows do not contribute. The entire identifier is the symbol key; exchanges never share state.
- Trading date/time (columns 26/23) are event time; each missing component falls back to system date/time (2/3). If the combined timestamp is invalid, retry both system fields. Capture/system time is not used to order windows except as this fallback. Interpret the wall time in the explicitly selected timezone, truncate fractional seconds to milliseconds, and reject pre-epoch times. For ambiguous DST times the earlier offset is used; timezone gap behavior should be checked against the source and runtime before using such rows as evidence.
- Five-minute windows are aligned to UTC epoch/midnight boundaries. Close is the trade with greatest event millisecond, breaking ties by the last original source ordinal across files. All eligible rows are included, including duplicates: there is no deduplication or lateness exclusion. This defines ideal complete-input truth, not a live-policy expectation.
- Empty windows emit nothing and do not update state. Each symbol starts with both EMAs at zero. For each close `p`, update `EMA38 += (2/39)*(p-EMA38)` and `EMA100 += (2/101)*(p-EMA100)`. BUY requires previous difference <= 0 and new difference > 0; SELL requires previous >= 0 and new < 0. Equality itself and non-crossing continuations do not emit. The last nonempty window is always finalized at end of input.

## Worked example

In the fixture, `ABC.X` first closes at 39: EMA38 = `39 * 2/39 = 2`, EMA100 = `39 * 2/101 = 0.7722772277227723`. The previous difference is zero and the new difference is positive, so the result is BUY. The next close is 1: EMA38 = `1.9487179487179487`, EMA100 = `0.7767865895500441`; both remain ordered fast > slow, so no signal. `ABC.Y` starts independently.

The test's longer rational-arithmetic example starts at 39, follows with 31 closes of 0.01 and then 1000. At window 30 EMA38 = `0.42017254521453756`, EMA100 = `0.42833824515313035`, crossing to SELL. At window 32 the values are `51.66072071952578` and `20.213716610209374`, crossing back to BUY. The test asserts both directions and intermediate continuation rather than deriving expected values from pipeline output.

## Current behavior observations

`event-emitter/.../DataLoader.java` skips 13 lines, reads the fields above, and applies the fallback described above. `EventEmitter.java` defaults to Europe/Berlin. `flink/.../EventJson.java` excludes zero `last`; `PriceIndicators.java` chooses the latest accepted event time and last accepted arrival on ties, starts both EMAs at zero, skips empty windows, and emits BUY/SELL on crossovers. These are observations, not proof of the reference interpretation.

The default deduplication policy is NONE; optional policies and processing-time TTL are described in `docs/policies.md`. Live watermarks, partition idleness, quote events, lateness and configured grace can exclude an otherwise eligible close, and their outcomes depend on actual arrival and execution. A bounded replay flushes its final windows; an idle continuous run is not an end-of-input flush. Do not use this complete-input output as a prescribed live-policy result. Exact reference replay must separately establish its input multiset, tie order, zero late exclusions and final flush.
