"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


DATA_COLS = [
    "births",
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


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("maternal_mortality")

    # Read table from meadow dataset.
    tb = ds_meadow["maternal_mortality"].reset_index()

    # drop rows where parameter is mmr_mean or pm_mean
    tb = tb[~tb["parameter"].str.contains("mean")]
    # drop uncertainty intervals (thresholds 10% and 90%)
    tb = tb.drop(columns=["_0_1", "_0_9"])

    tb = tb.pivot_table(index=["country", "year"], columns=["parameter"], values="_0_5").reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    # Add origins to columns.
    tb = add_origins(tb, DATA_COLS)
    tb = tb.rename(columns={"mmr_rate": "mm_rate"})

    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def add_origins(tb, cols):
    for col in cols:
        tb[col] = tb[col].copy_metadata(tb["country"])
    return tb
