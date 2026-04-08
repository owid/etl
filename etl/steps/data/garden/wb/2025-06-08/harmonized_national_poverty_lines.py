"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


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

    # Rename country_code to country.
    tb = tb.rename(columns={"country_code": "country"}, errors="raise")

    # Keep relevant columns.
    tb = tb[["country", "year", "reporting_level", "welfare_type", "harm_npl"]]

    # Select only national data
    tb = tb[tb["reporting_level"] == "national"].reset_index(drop=True)

    # Select only latest year for each country.
    tb = tb.sort_values(by=["country", "year"]).drop_duplicates(subset=["country"], keep="last").reset_index(drop=True)

    # Drop reporting_level and welfare_type columns.
    tb = tb.drop(columns=["reporting_level", "welfare_type"], errors="raise")

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
