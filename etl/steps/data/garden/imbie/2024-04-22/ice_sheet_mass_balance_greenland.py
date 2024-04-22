"""Load a meadow dataset and create a garden dataset."""


from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("ice_sheet_mass_balance_greenland")

    # Read table from meadow dataset.
    tb = ds_meadow["ice_sheet_mass_balance_greenland"].reset_index()

    #
    # Process data.
    #
    # Harmonize country names.
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    # Years are given as decimals. For this exercise, get the average cumulative mass balance for each year.
    tb["year"] = tb["year"].astype(int)
    tb = tb.groupby(["country", "year"], observed=True, as_index=False).agg({"cumulative_mass_balance__gt": "sum"})

    # Format conveniently.
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
