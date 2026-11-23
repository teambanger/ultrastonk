# Dataset profiling pilot

This is a partial implementation of ticket 02, not a verified full-week profile
or a peak fixture. The provider manifest is pinned in
`scripts/official-week-manifest.json` (Zenodo DOI 10.5281/zenodo.6382482,
CC BY-NC-SA 4.0). It orders the files by day; Zenodo's API file listing is not
chronological. Keep the source CSVs outside Git and public artifacts.

The experimental profiler accepts a JSON manifest with `doi`, `license`, and a
`files` array containing `key`, `size`, and `checksum` (`md5:...`). It verifies
each file's size and MD5, records SHA-256 and row counts, and reports partial
aggregates and provisional capture-time peaks. Run, after acquiring all files:

```sh
python3 scripts/profile_week.py --manifest scripts/official-week-manifest.json --data-dir /path/to/private-data --scratch-dir /path/to/disk-backed-scratch --output /path/to/private-results/profile.json
python3 -m unittest scripts.test_profile_week
```

The scratch and output directories must exist. The timestamp index can be large;
size disk needs with a representative weekday first. Do not use a memory-backed
temporary filesystem for a full-week run. Source files must remain unchanged
between verification and scanning. The profiler reads each file twice.

Capture time is the system date/time as a naive wall clock; it is not the replay
event timestamp. Missing or invalid capture times are excluded and counted, not
filled from trade time. Price-bearing rows are a provisional strictly-positive,
finite `Last` definition, not a confirmed trade or query eligibility definition.
Field presence overlaps row categories. See the `definitions` object in output
for denominators and tie handling. A peak from a partial manifest is not the
official week peak.

## Sunday pilot

On the official 14 November file, the published 1,905,658-byte size and MD5
`adfc75f72d9348209d602baa72af44b5` matched; local SHA-256 was
`2eae798511118974140a9790c6c3996c5c584c1553ba763b70d9fb3aa01f6cf4`.
It contained 26,113 data rows, of which 20,640 had valid capture timestamps and
5,473 did not. The pilot used a one-file manifest with the same schema and
provider values; it is not evidence of the week's peak. On this host it took
0.22 seconds and 27,704 KiB peak RSS. Sunday has no populated last-price fields,
so it is not representative for sizing a weekday or validating trade rates.

## Outstanding

The other six files were not acquired or verified. Complete-week profiling,
event-timestamp fallback and replay screening reconciliation, rolling-rate
series, per-day breakdowns, capture-ordered fixture extraction, exchange mix and
pre-roll/trailing context remain to be implemented and validated. In particular,
the current `raw_peak` is not a reproducible fixture definition and must not be
used as the ticket's final peak selection. Tests use small synthetic CSVs and do
not validate the profiler against a complete official weekday.
