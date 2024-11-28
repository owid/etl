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
    tb = ds_meadow.read("renewable_power_generation_costs")
    tb_solar_pv = ds_meadow.read("solar_photovoltaic_module_prices", reset_index=False)

    #
    # Process data.
    #
    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    ####################################################################################################################
    # Netherland's onshore wind LCOE in 2017 is unreasonably high.
    # In the previous version of the data, it was 0.087USD/kWh, and in the new version it is 0.84USD/kWh.
    # Looking at the report (Figure 2.13, page 78) that abrupt spike does not occur.
    # So, for now, I will remove that data point and contact IRENA.
    error = "Expected potentially spurious data point for Netherlands in 2017 onshore wind LCOE. It may have been removed in the latest update, so this code can be removed."
    assert tb.loc[(tb["country"] == "Netherlands") & (tb["year"] == 2017)]["onshore_wind"].item() > 0.8, error
    tb.loc[((tb["country"] == "Netherlands") & (tb["year"] == 2017)), "onshore_wind"] = None
    ####################################################################################################################

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
