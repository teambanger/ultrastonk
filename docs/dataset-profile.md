# Dataset profiling

The provider manifest is pinned in
`scripts/official-week-manifest.json` (Zenodo DOI 10.5281/zenodo.6382482,
CC BY-NC-SA 4.0). It orders the files by day; Zenodo's API file listing is not
chronological. Keep the source CSVs outside Git and public artifacts.

The profiler accepts a JSON manifest with `doi`, `license`, and a
`files` array containing `key`, `size`, and `checksum` (`md5:...`). It verifies
each file's size and MD5, records SHA-256 and row counts, and reports partial
aggregates and provisional capture-time peaks. Run, after acquiring all files:

```sh
python3 scripts/profile_week.py --manifest scripts/official-week-manifest.json --data-dir /path/to/private-data --scratch-dir /path/to/disk-backed-scratch --output /path/to/private-results/profile.json --series /path/to/private-results/rates.jsonl
python3 scripts/profile_fixture.py --profile /path/to/private-results/profile.json --manifest scripts/official-week-manifest.json --data-dir /path/to/private-data --scratch-dir /path/to/disk-backed-scratch --output /path/to/private-results/fixture.json
python3 -m unittest scripts.test_profile_week scripts.test_profile_rates scripts.test_profile_fixture
```

The scratch and output directories must exist. The timestamp index can be large;
size disk needs with a representative weekday first. Do not use a memory-backed
temporary filesystem for a full-week run. Source files must remain unchanged
between verification and scanning. The profiler reads each file twice. The
fixture command independently checks the source SHA-256 hashes against the
profile before scanning. Generation commands include local paths; sanitize
those paths before publishing artifacts while preserving reproducibility.

Capture time is the system date/time as a naive wall clock; it is not the replay
event timestamp. Missing or invalid capture times are excluded and counted, not
filled from trade time. Price-bearing rows are a provisional strictly-positive,
finite `Last` definition, not a confirmed trade or query eligibility definition.
Field presence overlaps row categories. See the `definitions` object in output
for denominators and tie handling. A peak from a partial manifest is not the
official week peak.

The [official paper](https://arxiv.org/html/2206.13237v1#S3) calls these system
fields the date and time of the last received update. It does not identify the
precise timestamping point or clock accuracy, so "capture time" here is a
profiling convention, not proof of a network capture instant. The paper says
CEST, while the recorded week is after the 2021 transition to Central European
standard time. No absolute timezone conversion is made here. See
`benchmarks/query-truth.md` for the separate event-time reference contract.

`rates.jsonl` contains sparse nonempty one-second buckets and the count/rate
over the current and preceding 299 whole seconds. The five-minute peak uses
exact timestamp precision instead, so it is not derived from these aligned
rolling buckets. Raw and provisional price-bearing counts remain separate.
The fixture records source-order disorder and gaps as well as timestamp-sorted
gaps; timestamp ties use source ordinal. It records nearest eligible context,
and available context ranges, not a fully warmed EMA state. Its inputs are tied to the profile and manifest
SHA-256 values, and no raw rows are exported.

## Sunday pilot

On the official 14 November file, the published 1,905,658-byte size and MD5
`adfc75f72d9348209d602baa72af44b5` matched; local SHA-256 was
`2eae798511118974140a9790c6c3996c5c584c1553ba763b70d9fb3aa01f6cf4`.
It contained 26,113 data rows, of which 20,640 had valid capture timestamps and
5,473 did not. The pilot used a one-file manifest with the same schema and
provider values; it is not evidence of the week's peak. On this host it took
0.22 seconds and 27,704 KiB peak RSS. Sunday has no populated last-price fields,
so it is not representative for sizing a weekday or validating trade rates.

## Full week

All seven official files were acquired, verified against the provider size and
MD5, and profiled on 2026-11-23. The manifest SHA-256 is
`b2cf2fb7a1f5d665deb92755ebafb32c73808ed06dbe1b497b070bfb6767a034`; the
profile's `generation_command` records the exact invocation. Local paths in that
command must be sanitized before publication.

| Day | Rows | FR | NL | ETR |
| --- | ---: | ---: | ---: | ---: |
| 08 | 53,994,554 | 29,549,814 | 14,457,112 | 9,987,628 |
| 09 | 58,811,870 | 32,781,403 | 15,790,258 | 10,240,209 |
| 10 | 64,591,844 | 35,630,011 | 18,532,858 | 10,428,975 |
| 11 | 55,547,846 | 30,332,082 | 15,172,700 | 10,043,064 |
| 12 | 56,111,183 | 30,012,686 | 16,031,917 | 10,066,580 |
| 13 | 33,398 | 13,823 | 3,487 | 16,088 |
| 14 | 26,113 | 11,259 | 2,858 | 11,996 |
| **Week** | **289,116,808** | **158,331,078** | **79,991,190** | **50,794,540** |

All 289,116,808 rows have a valid width and a recognized exchange. Security
types are E 239,521,477 and I 49,595,331. The provisional price-bearing split is
59,391,670 trade-like and 229,725,138 non-trade rows. Capture screening excludes
67,056 rows with an invalid system date/time; the replay-screening emitter sees
67,049 missing-time rows and 7 invalid-timestamp rows. Timestamp fallback for
emitted rows is 148,380,530 system, 135,750,538 mixed, and 4,918,684 trading.

The highest rolling five-minute raw capture peak is
**1,223,873 rows** in `2021-11-09T15:58:01.000000000` to
`2021-11-09T16:03:01.000000000` (about 4,080 rows/s). The separate trade peak is
**130,524 rows** in `2021-11-10T17:34:37.000000000` to
`2021-11-10T17:39:37.000000000` (about 435 rows/s). The largest aligned
one-second bucket is 23,016 rows at `2021-11-10T17:40:00.000000000`. Symbol
concentration is long-tailed: 5,502 distinct symbols, with the top symbol
(RDSA.NL) at 6,903,592 rows (2.39%), the top 10 at 15.71%, and the top 100 at
58.53%. The rate series has 222,844 nonempty one-second buckets with SHA-256
`12883bc128899a4a9d63b8851fef120412d075fc22ad0ef4395c527d458a98e2`.

The raw peak and the one-second burst fall on different days and intervals, so a
single "peak" number must state which definition it uses. The raw peak is the
capture-rate workload; the trade peak is the provisional trade-rate workload.

The fixture definition was generated from the verified profile and links to it
by SHA-256 (`profile_sha256`
`c6c38203f027dcbf6b842b60add002fa7c269366a0e59b97b90a7111aa34b358`, manifest
SHA-256 as above). It selects 1,223,873 rows from the raw peak in manifest order:
FR 651,418, NL 411,885, ETR 160,570. Source order is not capture order: 24
adjacent source-order pairs decrease in timestamp, and 1,223,527 of 1,223,872
adjacent pairs share a timestamp. Sorted by timestamp with source ordinal as the
tie-break, the largest gap inside the interval is 1 second. The nearest eligible
pre-roll row is 6,833 rows at `2021-11-09T15:58:00.000000000`; the nearest
trailing row is 4,137 rows at `2021-11-09T16:03:01.000000000`. Available context
extends back to `2021-11-08T00:00:00.000000000` (96,966,362 rows) and forward to
`2021-11-14T23:50:37.000000000` (190,859,517 rows). No raw rows are exported.

## Evidence Status

All seven official files passed provider size and MD5 verification, the complete
week was profiled in one run, and the peak fixture was generated from that
verified profile. The profiler and fixture commands, manifest, and profile
SHA-256 values make the result reproducible from the source files. The peak and
distribution figures above are therefore week-level results, not a partial
pilot. Tests use small synthetic CSVs and validate the commands' behavior, not
the official-week values.

Limitations that remain: the "capture time" is the system wall-clock field, not
a proven network capture instant; rows with no valid system timestamp cannot be
placed into a capture interval and their exclusions must accompany any peak
claim; the trade definition is a provisional strictly-positive finite `Last`
proxy, not the official challenge eligibility rule; the replay screening counts
are transport-level approximations using wall-clock parsing and do not include
all downstream query validation; and no absolute timezone conversion is made
because the paper's "CEST" wording conflicts with the post-transition week.
