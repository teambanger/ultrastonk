# 02: Profile the official week and identify the real peak

**What to build:** A report writer can trace workload claims to a verified seven-day dataset profile and reproduce the selection of the peak fixture.

**Blocked by:** None (can start immediately).

**Status:** ready-for-agent

- [ ] Verify all seven official Nov 8-14, 2021 CSVs against the Zenodo file manifest for DOI `10.5281/zenodo.6382482`; retain file order, sizes, provider checksums, local hashes, exact row counts, and license. Keep raw data out of Git and public artifacts.
- [ ] Scan all seven files in bounded memory. Report day, exchange, security-type, and row-category counts, field availability and timestamp fallback, one-second bursts, rolling five-minute rates, symbol ranks and top-symbol shares. Distinguish field presence from exclusive row dispositions and raw-row rates from trade rates; record screening definitions for reconciliation with the query contract.
- [ ] Select the highest rolling five-minute raw-row capture interval across FR, NL, and ETR, with documented timestamp handling and deterministic ties. Record the trade peak separately if it differs.
- [ ] Export machine-readable aggregates and a reproducible fixture definition with capture-time order and gaps, exchange mix, and available context for pre-roll and trailing input. Link figures to the manifest and generation command.
