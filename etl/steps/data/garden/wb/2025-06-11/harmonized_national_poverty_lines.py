"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define old and new names for income groups
INCOME_GROUPS = {
    "Low income": "Low income",
    "Lower middle income": "Lower-middle income",
    "Upper middle income": "Upper-middle income",
    "High income": "High income",
}


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("harmonized_national_poverty_lines")

    # Read table from meadow dataset.
    tb = ds_meadow.read("harmonized_national_poverty_lines")

    #
    # Process data.
    #
    # Harmonize country names.
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
        country_col="country_code",
    )

    # Rename columns.
    tb = tb.rename(
        columns={
            "country_code": "country",
            "harm_npl": "harmonized_national_poverty_line",
            "incgroup": "income_group",
        },
        errors="raise",
    )

    # Keep only relevant columns.
    tb = tb[["country", "year", "harmonized_national_poverty_line", "income_group"]]

    # Rename income groups
    tb["income_group"] = tb["income_group"].replace(INCOME_GROUPS)

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
