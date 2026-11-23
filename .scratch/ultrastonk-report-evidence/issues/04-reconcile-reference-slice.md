# 04: Reconcile a reference Historical replay slice

**What to build:** An experimenter can run a representative recorded slice through the production queries and get an exact, reproducible oracle comparison including the last window.

**Blocked by:** 03 - Deliver live BUY/SELL to subscribers.

**Status:** ready-for-agent

- [ ] Use a slice from a verified official day containing timestamp reversals and a final nonempty window. Reconcile source dispositions, acknowledged publication, Kafka offsets and committed EMA/advisory results in fresh topics, using memory-bounded comparisons suitable for scaling to the week.
- [ ] Try ordinary bounded replay first. If eligible trades are excluded, validate a separately labeled event-time-ordered reference replay that preserves the replayable input multiset and original ordinal on ties, does not precompute closes or advisories, and uses the same production query operators.
- [ ] Require zero eligible late exclusions and zero oracle mismatches in the chosen reference mode, including the final window. Check that errors or incomplete accounting cannot yield a pass.
- [ ] Save the mode decision, slice commands, settings and counts. Use observed input/output topic, checkpoint and scratch volumes to preflight retention and storage for the full-week run, including sorting if needed.
