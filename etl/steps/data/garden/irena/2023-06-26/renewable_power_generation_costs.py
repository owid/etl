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
    # Load meadow dataset and read its tables.
    ds_meadow = cast(Dataset, paths.load_dependency("renewable_power_generation_costs"))
    tb = ds_meadow["renewable_power_generation_costs"].reset_index()
    tb_solar_pv = ds_meadow["solar_photovoltaic_module_prices"]

    #
    # Process data.
    #
    # Harmonize country names.
    tb: Table = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb, tb_solar_pv], default_metadata=ds_meadow.metadata)
    ds_garden.save()
