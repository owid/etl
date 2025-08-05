"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and table
    ds_meadow = paths.load_dataset("deaths_karlinsky")
    tb = ds_meadow.read("deaths_karlinsky")

    # Load population
    ds_pop = paths.load_dataset("population")

    #
    # Process data.
    #
    # drop and rename columns
    tb = tb.drop(columns=["continent", "source"])
    tb = tb.rename(columns={"country_name": "country"})

    # harmonize country names
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )

    # Format
    tb = tb.format(["country", "year"])

    # sanity checks
    _sanity_checks(tb, ds_pop)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def _sanity_checks(tb: Table, ds_pop) -> None:
    # check columns
    columns_expected = {
        "death_comp",
        "expected_deaths",
        "expected_gbd",
        "expected_ghe",
        "expected_wpp",
        "reg_deaths",
        "expected_confidence_score",
    }
    columns_new = set(tb.columns).difference(columns_expected)
    if columns_new:
        raise ValueError(f"Unexpected columns {columns_new}")

    # ensure percentages make sense (within range [0, 100])
    columns_perc = ["death_comp"]
    for col in columns_perc:
        assert all(tb[col] <= 100), f"{col} has values larger than 100%"
        assert all(tb[col] >= 0), f"{col} has values lower than 0%"

    # Add population to table for sanity check
    columns_absolute = [col for col in tb.columns if col not in columns_perc]
    tb_ = tb.reset_index()
    tb_ = geo.add_population_to_table(tb_, ds_population=ds_pop)

    # Check NAs in population only for Micronesia
    mask = tb_["population"].isna()
    assert set(tb_.loc[mask, "country"].unique()) == {"Micronesia"}, "Only Micronesia expected to have population=NA"
    assert mask.sum() == 2, "Only 2 occurrences of NAs accepted in population"

    # Safely drop NAs
    tb_ = tb_.dropna(subset=["population"])

    # Actual sanity check: Ensure absolute values make sense (positive, lower than population)
    for col in columns_absolute:
        x = tb_.dropna(subset=[col])
        assert all(
            x[col] < 0.2 * x["population"]
        ), f"{col} contains values that might be too large (compared to population values)!"
