"""Load a meadow dataset and create a garden dataset."""

from typing import cast

from owid.catalog import Dataset, Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("deaths_karlinsky"))

    # Read table from meadow dataset.
    tb = ds_meadow["deaths_karlinsky"].reset_index()

    #
    # Process data.
    #
    # drop and rename columns
    tb = tb.drop(columns=["continent", "source"])
    tb = tb.rename(columns={"country_name": "country"})

    # harmonize country names
    tb: Table = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index()

    # sanity checks
    _sanity_checks(tb)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def _sanity_checks(tb: Table) -> None:
    # check columns
    columns_expected = {
        "death_comp",
        "expected_deaths",
        "expected_gbd",
        "expected_ghe",
        "expected_wpp",
        "reg_deaths",
    }
    columns_new = set(tb.columns).difference(columns_expected)
    if columns_new:
        raise ValueError(f"Unexpected columns {columns_new}")

    # ensure percentages make sense (within range [0, 100])
    columns_perc = ["death_comp"]
    for col in columns_perc:
        assert all(tb[col] <= 100), f"{col} has values larger than 100%"
        assert all(tb[col] >= 0), f"{col} has values lower than 0%"

    # ensure absolute values make sense (positive, lower than population)
    columns_absolute = [col for col in tb.columns if col not in columns_perc]
    tb_ = tb.reset_index()
    tb_ = geo.add_population_to_dataframe(tb_)
    for col in columns_absolute:
        x = tb_.dropna(subset=[col])
        assert all(
            x[col] < 0.2 * x["population"]
        ), f"{col} contains values that might be too large (compared to population values)!"
