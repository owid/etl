"""Create a dataset of renewable electricity capacity using IRENA's Renewable Electricity Capacity and Generation."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Regions for which aggregates will be created.
REGIONS = [
    "North America",
    "South America",
    "Europe",
    # European Union (27) is already included in the original data.
    # "European Union (27)",
    "Africa",
    "Asia",
    "Oceania",
    "Low-income countries",
    "Upper-middle-income countries",
    "Lower-middle-income countries",
    "High-income countries",
]


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load dataset from Meadow and read its main table.
    ds_meadow = paths.load_dataset("renewable_electricity_capacity")
    tb = ds_meadow["renewable_electricity_capacity"].reset_index()

    # Load regions dataset.
    ds_regions = paths.load_dataset("regions")

    # Load income groups dataset.
    ds_income_groups = paths.load_dataset("income_groups")

    #
    # Process data.
    #
    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Reshape dataframe to have each technology as a separate column
    tb = tb.pivot(index=["country", "year"], columns=["technology"], values="capacity", join_column_levels_with="")

    # For convenience, remove parentheses from column names.
    tb = tb.rename(columns={column: column.replace("(", "").replace(")", "") for column in tb.columns}, errors="raise")

    ####################################################################################################################
    # The latest Statistical Review reports a very high wind generation in Saudi Arabia in 2022.
    # The reported value would be physically impossible, given the reported capacity from IRENA for 2022.
    # However, IRENA has recently updated their data, reporting a much higher capacity for Arabia, of 403MW in 2022.
    # https://www.irena.org/Publications/2024/Mar/Renewable-capacity-statistics-2024
    # That would be consistent with the reported generation.
    # For now, I will remove the old data point for 2022 (which was an estimate by IRENA).
    # Next time capacity data is updated, the following assertion will fail, and we will remove this part of the code.
    wind_columns = ["Onshore wind energy", "Wind energy"]
    error = (
        "IRENA's reported wind capacity for Saudi Arabia used to be 3MW (incompatible with generation reported by "
        "the Statistical Review). This may have changed now. Remove this code."
    )
    assert tb.loc[(tb["country"] == "Saudi Arabia") & (tb["year"] == 2022), "Onshore wind energy"].item() == 3.25, error
    tb.loc[(tb["country"] == "Saudi Arabia") & (tb["year"] == 2022), wind_columns] = None
    ####################################################################################################################

    # Add region aggregates.
    tb = geo.add_regions_to_table(
        tb, regions=REGIONS, ds_regions=ds_regions, ds_income_groups=ds_income_groups, min_num_values_per_year=1
    )

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir=dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()
