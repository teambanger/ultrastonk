# 04: Reconcile a reference Historical replay slice

**What to build:** An experimenter can run a representative recorded slice through the production queries and get an exact, reproducible oracle comparison including the last window.

**Blocked by:** 03 - Deliver live BUY/SELL to subscribers.

**Status:** resolved

- [x] Use a slice from a verified official day containing timestamp reversals and a final nonempty window. Reconcile source dispositions, acknowledged publication, Kafka offsets and committed EMA/advisory results in fresh topics, using memory-bounded comparisons suitable for scaling to the week.
- [x] Try ordinary bounded replay first. If eligible trades are excluded, validate a separately labeled event-time-ordered reference replay that preserves the replayable input multiset and original ordinal on ties, does not precompute closes or advisories, and uses the same production query operators.
- [x] Require zero eligible late exclusions and zero oracle mismatches in the chosen reference mode, including the final window. Check that errors or incomplete accounting cannot yield a pass.
- [x] Save the mode decision, slice commands, settings and counts. Use observed input/output topic, checkpoint and scratch volumes to preflight retention and storage for the full-week run, including sorting if needed.

## Answer

Implemented in commit `5543e2b`, merged with integration tip `59a49c6` (merge `754be18`).

The verified day-12 file (size and MD5 match the official manifest) supplied a contiguous 1,605,199-row slice spanning 08:55-09:10 Europe/Berlin. It has 835,332 event-time reversals (26,316 eligible, maximum 996 ms) and three nonempty windows; the final window is 09:05 (`1636704300000`).

Ordinary capture-order bounded replay was tried first and is the chosen mode: 1,605,197 input records, zero emitter failures, zero audit records, 250,716 committed prices, 7,937 EMA pairs and 3,671 advisories. The bounded comparator matched all 11,608 oracle records exactly, including the final window. An event-time-ordered reference run was also executed as a labeled cross-check and passed identically; its stable `(event_ms, original ordinal)` sort was separately verified to preserve the oracle output. It is not the chosen path because it needs the additional sort scratch.

`benchmarks/reconcile_replay.py` streams source scans, extraction and normalization, uses disk SQLite for stable sorting and comparison, and gates the run on emitter completeness, input offsets, audit completeness, zero late exclusions, zero rejected inputs, committed price counts, exact oracle match and final-window presence. Unit tests deliberately break each check. `query_truth_compare.py --db` now provides the same disk-backed comparator outside the replay harness.

Observed storage was input 29.3 MB, output 27.7 MB, checkpoint 0.69 MB, sort scratch 203 MB and sorted CSV 135 MB. Byte-scaling the official 24.87 GB week estimates 291.4 M rows, about 46 GB peak (15% headroom) for the chosen capture-order path and about 117 GB including the event-time sort fallback, against 178 GiB free. Kafka's seven-day retention needs about 10.4 GB for full-week input and output.

Evidence: `benchmarks/reference-slice.md` and `benchmarks/evidence/reference-slice/manifest.json`. Raw CSVs and scratch databases remain outside Git.

## Comments

Limits: this is one weekday slice, not a completed full-week replay; storage totals are byte-scaled estimates for ticket 06. Replay was accelerated 1000x, so this is correctness and storage evidence, not throughput or latency. No failure/restart schedule was exercised. The bounded comparator reports exact counts but caps example rows. Capture-order is chosen for this slice; a week section with larger lateness may still require the event-time reference.
