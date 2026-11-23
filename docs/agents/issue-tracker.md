# Issue tracker: Local Markdown

Specs live at `.scratch/<feature>/spec.md`. Implementation tickets live
at `.scratch/<feature>/issues/<NN>-<slug>.md`, one file per ticket,
numbered from `01`. Record triage in a `Status:` line near the top (see
[`triage-labels.md`](triage-labels.md)) and append discussion under `## Comments`.

To publish, create the corresponding file. To fetch, read the cited
path or resolve the ticket number within its feature directory.

For Wayfinder, use `.scratch/<effort>/map.md` and numbered child tickets.
Record `Type:`, `Status:`, and `Blocked by:` on child tickets. Claim the
first unblocked, unclaimed child by number by saving `Status: claimed`
before work. To resolve it, append an `## Answer`, set `Status: resolved`,
and link the answer from the map's Decisions-so-far section.
