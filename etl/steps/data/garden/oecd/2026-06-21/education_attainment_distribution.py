"""Load OECD educational attainment data (no splicing with other sources).

Produces OECD-only indicators (all education levels, total and by sex), used as an input
by the long-run educational attainment splice (education/2026-06-21/long_run_educational_attainment)
and available on their own for OECD's observed coverage.
"""

from owid.catalog import Table

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("education_attainment_distribution")
    tb_oecd_all = ds_meadow.read("education_attainment_distribution")

    #
    # Process data.
    #
    tb_oecd_all = paths.regions.harmonize_names(tb_oecd_all)

    # Split OECD into total and by-sex.
    tb_oecd_total = tb_oecd_all[tb_oecd_all["sex"] == "total"].drop(columns=["sex"]).reset_index(drop=True)
    tb_oecd_by_sex = tb_oecd_all[tb_oecd_all["sex"] != "total"].reset_index(drop=True)

    tb_oecd_total = tb_oecd_total.format(["country", "year"], short_name=paths.short_name)

    tb_oecd_by_sex["sex"] = tb_oecd_by_sex["sex"].map({"female": "Women", "male": "Men"}).astype("category")
    tb_oecd_sex = tb_oecd_by_sex.format(["country", "year", "sex"], short_name=f"{paths.short_name}_by_sex")

    #
    # Sanity checks.
    #
    sanity_check_outputs([tb_oecd_total, tb_oecd_sex])

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb_oecd_total, tb_oecd_sex])
    ds_garden.save()


def sanity_check_outputs(tables: list[Table]) -> None:
    """Check all output tables for common data integrity issues."""
    for tb in tables:
        tb_flat = tb.reset_index()
        name = tb.metadata.short_name

        # No fully-NaN columns.
        nan_cols = tb_flat.columns[tb_flat.isna().all()].tolist()
        assert not nan_cols, f"[{name}] Fully-NaN columns: {nan_cols}"

        # No duplicate key rows.
        index_cols = [c for c in ["country", "year", "sex"] if c in tb_flat.columns]
        assert not tb_flat.duplicated(subset=index_cols).any(), f"[{name}] Duplicate rows on {index_cols}"

        # Share columns should be in [0, 100].
        share_cols = [c for c in tb_flat.columns if c.startswith("share_")]
        for col in share_cols:
            vals = tb_flat[col].dropna()
            if len(vals) == 0:
                continue
            assert vals.min() >= 0, f"[{name}] {col} has negative values (min={vals.min()})"
            assert vals.max() <= 100, f"[{name}] {col} exceeds 100 (max={vals.max()})"
