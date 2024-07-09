"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

REGIONS = [reg for reg in geo.REGIONS.keys() if reg != "European Union (27)"] + ["World"]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load data sets from Gapminder and UN
    ds_gm = paths.load_dataset("maternal_mortality", namespace="gapminder")
    ds_un = paths.load_dataset("maternal_mortality", namespace="un")
    ds_regions = paths.load_dataset("regions")
    ds_income = paths.load_dataset("income_groups")

    # Read table from meadow dataset.
    tb_gm = ds_gm["maternal_mortality"].reset_index()
    tb_un = ds_un["maternal_mortality"].reset_index()

    aggr = {"maternal_deaths": sum, "births": sum}

    # regional aggregates (before joining - since Gapminder only includes 14 countries)
    tb_un = geo.add_regions_to_table(
        tb=tb_un,
        regions=REGIONS,
        ds_regions=ds_regions,
        ds_income_groups=ds_income,
        aggregations=aggr,
        num_allowed_nans_per_year=5,
    )

    # calculate aggregated maternal mortality ratio for regions
    tb_un["mmr"] = tb_un.apply(lambda x: calc_mmr(x), axis=1)

    # join the two tables
    tb = tb_un.merge(tb_gm, how="outer", on=["country", "year"], suffixes=("_gm", "_un"))

    # combine rows, using UN data where available
    tb["maternal_deaths"] = tb["maternal_deaths_un"].combine_first(tb["maternal_deaths_gm"])
    tb["mmr"] = tb["mmr_un"].combine_first(tb["mmr_gm"])
    tb["live_births"] = tb["live_births"].combine_first(tb["births"])
    tb["mm_rate"] = tb["mm_rate_un"].combine_first(tb["mm_rate_gm"])

    # TODO - figure out how to join metadata here - currently all joined columns only have metadata from the GM dataset

    print(tb["maternal_deaths"].metadata)
    print(tb["mmr"].metadata)
    print(tb["live_births"].metadata)
    print(tb["mm_rate"].metadata)

    # since this is the long run dataset, drop all columns not in both datasets
    cols_to_keep = ["country", "year", "maternal_deaths", "mmr", "live_births", "mm_rate"]
    tb = tb[cols_to_keep]

    # index and format columns
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_gm.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def calc_mmr(tb_row):
    """If country is a region, calculate the maternal mortality ratio, else return MMR"""
    if tb_row["country"] in REGIONS:
        return (tb_row["maternal_deaths"] / tb_row["births"]) * 100_000
    return tb_row["mmr"]
