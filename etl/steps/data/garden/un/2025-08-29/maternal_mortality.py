"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder

REGIONS = ["Africa", "Asia", "Europe", "North America", "Oceania", "South America"]

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


DATA_COLS = [
    "births",
    # "matcoviddeaths",
    # "coviddeaths",
    # "covidmmr",
    "hiv_related_indirect_maternal_deaths",
    "hiv_related_indirect_mmr",
    "hiv_related_indirect_percentage",
    "lifetime_risk",
    "lifetime_risk_1_in",
    "maternal_deaths",
    "mmr",
    "mmr_rate",
    "pm",
]


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("maternal_mortality")
    ds_regions = paths.load_dataset("regions")

    # Read table from meadow dataset.
    tb = ds_meadow["maternal_mortality"].reset_index()

    # drop rows where parameter is mmr_mean or pm_mean
    tb = tb[~tb["parameter"].str.contains("mean", na=False)]
    # include only point estimate (estimation midpoint), drop uncertainty intervals (thresholds 10% and 90%)
    tb = tb.drop(columns=["_0_1", "_0_9"])

    tb = tb.pivot_table(index=["country", "year"], columns=["parameter"], values="_0_5").reset_index()
    tb = tb.drop(columns=["matcoviddeaths", "coviddeaths", "covidmmr"])
    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # The MM rate is given by the UN as deaths per person-years lived by females aged 15-49 in that period
    # To make it comparable with other sources, we multiply it by 100,000 to get deaths per 100,000 person-years (roughly per 100,000 women)
    tb["mmr_rate"] = tb["mmr_rate"] * 100_000

    # Add origins to columns.
    tb = add_origins(tb, DATA_COLS)

    aggr = {"maternal_deaths": "sum", "births": "sum", "hiv_related_indirect_maternal_deaths": "sum"}
    tb = geo.add_regions_to_table(
        tb=tb,
        regions=REGIONS,
        ds_regions=ds_regions,
        aggregations=aggr,
        frac_allowed_nans_per_year=0.3,
    )

    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def add_origins(tb: Table, cols: list) -> Table:
    for col in cols:
        tb[col] = tb[col].copy_metadata(tb["country"])
    return tb
