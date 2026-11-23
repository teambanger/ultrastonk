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

## Evidence Status

The Sunday-only pilot is not a full-week profile and must not be used to select
the week's peak. A final week-level claim requires all seven files to pass
provider verification, a successful complete run, and a fixture definition
derived from that run. Tests use small synthetic CSVs and do not by themselves
validate any official-week result. Rows with no valid system timestamp cannot be
placed into a capture interval and their exclusions must accompany any peak
claim. The replay screening counts are transport-level approximations using
wall-clock parsing and do not include all downstream query validation.
