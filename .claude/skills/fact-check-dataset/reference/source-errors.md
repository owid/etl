# When a value looks wrong

Read this when a cross-check fails, an identity does not hold, or you are about to call something a source error. It decides what counts as confirmed and where the fix goes. The two routes to a **confirmed source error** are in [SKILL.md](../SKILL.md) → *Tolerance*.

## Internal accounting identities beat external sources

When a producer publishes components *and* their aggregate, check that they reconcile — a contradiction inside the producer's own release is arithmetic, needs no independent source, and can't be waved away as a methodology difference. It also survives the common case where every external source is bot-blocked. This is confirmation **route (b)**: it settles a source error on its own, provided the guards hold:

- every term of the identity comes from the **same release and vintage** (never mix an old download's share with a new download's rate);
- the terms are defined so the identity holds **by construction** (shares that sum to 1, urban + rural weighted by the same population split);
- the contradiction is far beyond rounding.

Published regional aggregates are such an identity only when the producer documents how they are built and the release holds every input (its membership list, every member's row, the weights or numerators and denominators): rebuild each aggregate that way, and when one region alone misses, solve for its dominant member, which exposes a country series revised in the aggregates but not in the country rows.

Two habits that make this reusable: sweep the identity across **all** entities, not just the suspicious one, to prove the error is isolated rather than systematic; and check the producer's footnote table (`WDIfootnote.csv` and equivalents) — an unqualified bad value is a stronger finding than a flagged one.

One more guard before anything reaches the corrections route: the contradiction proves *an* error exists among the identity's terms — it does not by itself say **which** term is wrong. Identify the bad term with evidence beyond the identity: the entity's own adjacent years (the term solved from the identity matches its own prior-year value, while the other terms sit in line with their own histories), the all-entity sweep isolating a single term, or a footnote. If nothing singles out one term, the finding is still a confirmed producer error — report it and hand it to producer follow-up, but don't guess which value to overwrite in `corrections.yml`.

## A mismatch that matches exactly under swapped names is a label error

It can be on either side. When a comparison against the producer's own table fails for two related rows (two subregions, two sexes, two variants) and each row's values equal the *other* row's to the last decimal, don't treat it as our pipeline being wrong. Settle which side has the labels crossed with an independent reconstruction — rebuild both aggregates from their members, or check the populations each label implies against the table's own totals — before touching anything. If it is the producer's document, that is a confirmed producer error to report; if it is ours, it is an entity-mapping bug.

## Attribution before routing

Before routing any confirmed bad value, read the raw snapshot to determine where it entered:

```python
from etl.snapshot import Snapshot

tb = Snapshot("<ns>/<version>/<file>").read()  # picks the reader from the file's format
```

Use the format-specific `read_csv` / `read_excel` / `read_json` only when auto-detection needs overriding, and filter to the rows in question: a whole large workbook takes minutes to load. Present in the source file → source error (corrections route). Absent → our processing introduced it (trace snapshot → meadow → garden and fix the step).

## Writing the correction

- A confirmed source error goes in `<short_name>.corrections.yml` next to the garden step, applied with `tb = paths.apply_corrections(tb)` (format: `etl/data_corrections.py`). Fill `reason`, `producer` and `status`, add an `expect` guard, and tell the user to notify the producer and record the `reported:` date.
- `override` values on categorical columns must come from the source's *current* vocabulary — assigning a retired label fails with `Cannot setitem on a Categorical`; choose the current-vocabulary value that yields the same published output.
- Suspicious but unconfirmed (independent sources disagree with each other, or methodology plausibly explains the gap): a "verify manually" item in the report, never a correction.
