# 02: Profile the official week and identify the real peak

**What to build:** A report writer can trace workload claims to a verified seven-day dataset profile and reproduce the selection of the peak fixture.

**Blocked by:** None (can start immediately).

**Status:** resolved

- [x] Verify all seven official Nov 8-14, 2021 CSVs against the Zenodo file manifest for DOI `10.5281/zenodo.6382482`; retain file order, sizes, provider checksums, local hashes, exact row counts, and license. Keep raw data out of Git and public artifacts.
- [x] Scan all seven files in bounded memory. Report day, exchange, security-type, and row-category counts, field availability and timestamp fallback, one-second bursts, rolling five-minute rates, symbol ranks and top-symbol shares. Distinguish field presence from exclusive row dispositions and raw-row rates from trade rates; record screening definitions for reconciliation with the query contract.
- [x] Select the highest rolling five-minute raw-row capture interval across FR, NL, and ETR, with documented timestamp handling and deterministic ties. Record the trade peak separately if it differs.
- [x] Export machine-readable aggregates and a reproducible fixture definition with capture-time order and gaps, exchange mix, and available context for pre-roll and trailing input. Link figures to the manifest and generation command.

## Answer

Integrated as merge `c820181` from `dataset-profiling` tip `347742f`. All seven files are held outside Git at `/mnt/shared/data/official-week-Ij8jlfeA`; every file matches the manifest size and provider MD5 (re-verified independently during integration). Full-week scan: **289,116,808** rows in bounded memory, matching the dataset's published scale; security-type counts reproduce the paper's 239.5M equities / 49.6M indices, and exchange mix is FR 158,331,078 / NL 79,991,190 / ETR 50,794,540.

Headline selections (from `week-profile.json`, SHA-256 `c6c38203…`, linked to manifest SHA-256 `b2cf2fb7…`):
- Raw capture peak: **1,223,873 rows** in `2021-11-09T15:58:01` → `16:03:01`, ~4,079 rows/s.
- Separate trade peak: 130,524 rows in `2021-11-10T17:34:37` → `17:39:37`; one-second burst 23,016 at `17:40:00`.
- 67,056 rows lack a valid system timestamp and are excluded and counted; 289,049,752 rows carry a usable capture time.
- Fixture (`week-fixture.json`): FR 651,418 / NL 411,885 / ETR 160,570; 24 source-order timestamp decreases, 1,223,527 of 1,223,872 adjacent pairs tied at the same millisecond, max timestamp-sorted gap 1 s; nearest pre-roll 6,833 rows and trailing 4,137 rows, with 96,966,362 pre-roll and 190,859,517 trailing eligible rows available.

Validation: 14 profiling tests and 17 benchmark tests pass.

## Comments

Limits that must travel with any report claim: "capture time" is the CSV system wall-clock field, not a proven network capture instant; the trade definition is a provisional strictly-positive finite `Last` proxy, not the official challenge eligibility rule; replay-screening counts are transport-level approximations; and no absolute timezone conversion is applied because the paper's "CEST" wording conflicts with the post-DST week. Peak claims hold only for the verified manifest revision.
