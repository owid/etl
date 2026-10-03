---
paths:
  - "etl/steps/**/*.py"
  - "snapshots/**/*.py"
---

# Sanity checks in ETL steps and snapshots

Silent data corruption is one of the easier bugs to miss. A step can run cleanly, pass type checks, and ship to staging while producing wrong numbers — and by the time someone notices on a chart, the bad data may already be live. Sanity checks are how we catch that at build time instead.

**Write them. Make them strict.** Every garden step that does anything more than a straight load-and-format should assert its assumptions about both the input it received and the output it produced. The bar isn't "would I notice this on a chart" — it's "could this go wrong, and if it did, would the step still finish without complaining?". If yes, that's a check.

### Where they go

- **Garden** is the main home. The input tables, the transformations, and the output table are all in one place, and that's where business logic lives. Put one `sanity_check_inputs(...)` right after loading meadow tables (before any transform), and one `sanity_check_outputs(...)` right before `paths.create_dataset(...)`. Call both from `run()`. See `etl/steps/data/garden/rff/2026-06-10/emissions_weighted_carbon_price.py` for a clean, recent example.
- **Snapshot** usually doesn't need checks — most snapshots just download and write. But when the snapshot itself does non-trivial parsing (PDF tables, custom binary formats, scraping), add integrity checks right before `snap.create_snapshot(...)`. If the parser can produce silently-wrong rows, the snapshot is the only place to catch it before the bad rows leak into meadow.
- **Meadow** should stay light. If you find yourself wanting checks there, it usually means the logic belongs in garden instead.

### How they look

Use plain `assert` with a clear error message. Hard-fail is the default — let the step crash loudly. Save `log.warning(...)` for checks that flag suspicious patterns the maintainer should *review* but that aren't always wrong (e.g., a country dropping to zero in the latest year — could be a real signal, could be incomplete reporting).

```python
def sanity_check_inputs(tb_economy: Table, tb_coverage: Table) -> None:
    assert set(tb_economy["jurisdiction"]) == set(tb_coverage["jurisdiction"]), "Jurisdictions don't match between the two input tables."
    assert not tb_economy.duplicated(subset=["jurisdiction", "year"]).any(), "Duplicate (jurisdiction, year) rows in economy table."
    price_cols = [c for c in tb_economy.columns if c not in ["jurisdiction", "year"]]
    assert tb_economy[price_cols].min().min() >= 0, "Negative price found — source error or unit mistake."


def sanity_check_outputs(tb: Table) -> None:
    assert tb.columns[tb.isna().all()].empty, "Output has a fully-NaN column."
    # Soft signal — surface for review, don't fail.
    dropped = sorted(set(tb[tb["year"] == tb["year"].max()].query("value == 0")["country"]))
    if dropped:
        log.warning(f"Countries that dropped to zero in the latest year: {dropped}")
```

### Categories worth checking

Pick whichever apply to your step. Don't write all of these by default — write the ones that would catch a real failure mode for this dataset.

- **Shape / schema invariants.** Set-equality on the columns, categories, subcategories, variables, or any other identifier list you expect to be stable across versions. (`set(tb["category"]) == set(EXPECTED_CATEGORIES)`.) Catches schema changes at the source.
- **Key uniqueness.** `assert not tb.duplicated(subset=[...]).any()` on whatever key columns the table is supposed to be unique on. Catches accidental row duplication from joins.
- **Value ranges.** Non-negative where it should be, within plausible bounds where you know them, no NaN where you don't expect any. Catches unit mistakes and parser drift.
- **Cross-table / cross-source agreement.** If two source tables both report the same key, their overlap should agree to the dollar (or within a documented tolerance). Catches misaligned extractions.
- **Sum reconciles with published total.** If the source publishes both the components and the total, assert that summing the components equals the total within tolerance. See `etl/steps/data/garden/emissions/2026-02-11/emissions_by_custom_sector.py:172` for an example. Catches row-shift extraction bugs and unit-mismatches.
- **No silent drops.** If you filter, dedupe, or aggregate, assert that the row count or aggregate matches what you'd expect. (`assert len(tb) == n_expected`.) Catches transforms that quietly lose data.
- **Coverage didn't shrink.** When updating a dataset version, check that you still have at least as many countries / quarters / categories as the previous version — a sudden drop is usually a parsing regression, not a real change.
- **Magnitude matches the previous version.** When rewriting or updating a step, compare output values against the previous live version — a silent unit regression (e.g. a dropped ×1e6 conversion) passes every schema check and ships wrong-by-a-million values. Source columns whose headers carry unit markers (`(mils)`, `(000)`, `%`) demand an explicit conversion in garden **plus** a magnitude assert (`assert 1e12 < tb[col].max() < 1e14`), so the conversion can't be lost in a future rewrite.

### When checks fail

Don't suppress the assertion to get the step to pass. Treat the failure as the signal it is: investigate, then either fix the upstream logic or — if the source genuinely changed — update the assertion (and document why in a `# NOTE:` comment near the check).
