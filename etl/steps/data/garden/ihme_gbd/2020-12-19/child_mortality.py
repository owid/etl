"""Garden step for IHME GBD 2020-12-19 child mortality (frozen vintage)."""

import json

from owid.catalog import Table

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    ds_meadow = paths.load_dataset("child_mortality")
    tb = ds_meadow["child_mortality"].reset_index()

    tb = exclude_countries(tb)
    tb = paths.regions.harmonize_names(
        tb,
        country_col="country",
        countries_file=paths.country_mapping_path,
    )

    # Keep only Both-sex rows. Rate metric is dropped — its definition is ambiguous in the raw export.
    tb = tb[(tb["sex"] == "Both") & (tb["metric_name"] != "Rate")]

    tb_p = tb.pivot(
        index=["country", "year"],
        columns=["measure_name", "age_group_name"],
        values="value",
    )
    tb_p.columns = ["_".join(col).strip() for col in tb_p.columns.values]
    tb_p = tb_p.reset_index()

    num_cols = [c for c in tb_p.columns if "Deaths" in c]
    prob_cols = [c for c in tb_p.columns if "Probability of death" in c]
    tb_p[num_cols] = tb_p[num_cols].round(0).astype(int)
    tb_p[prob_cols] = (100 * tb_p[prob_cols]).round(2)

    tb_p = tb_p.format(["country", "year"], short_name="child_mortality")

    ds_garden = paths.create_dataset(tables=[tb_p], default_metadata=ds_meadow.metadata)
    ds_garden.save()


def exclude_countries(tb: Table) -> Table:
    with open(paths.excluded_countries_path) as f:
        excluded = json.load(f)
    return tb.loc[~tb["country"].isin(excluded)]
