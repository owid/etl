"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its tables.
    ds_meadow = paths.load_dataset("renewable_power_generation_costs")
    tb = ds_meadow.read("renewable_power_generation_costs", safe_types=False)
    tb_solar_pv = ds_meadow.read("solar_photovoltaic_module_prices", reset_index=False, safe_types=False)

    #
    # Process data.
    #
    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Improve table formatting.
    tb = tb.format()

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb, tb_solar_pv], default_metadata=ds_meadow.metadata, check_variables_metadata=True
    )
    ds_garden.save()
