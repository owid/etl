"""Garden step for plastic waste data with country harmonization."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define regions for aggregation
REGIONS = [
    # Income groups
    "Low-income countries",
    "Lower-middle-income countries",
    "Upper-middle-income countries",
    "High-income countries",
    # Continents
    "Africa",
    "Asia",
    "Europe",
    "North America",
    "Oceania",
    "South America",
    # World
    "World",
]

REGIONS_COTTOM_ET_AL = [
    "Africa (UN M49)",
    "Americas (UN M49)",
    "Asia (UN M49)",
    "Europe (UN M49)",
    "High-income countries",
    "Low-income countries",
    "Lower-middle-income countries",
    "Oceania (UN M49)",
    "Upper-middle-income countries",
    "World",
]


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("cottom_plastic_waste")

    # Read tables from meadow dataset.
    tb = ds_meadow.read("cottom_plastic_waste")

    #
    # Process data.
    #
    # Harmonize country names for national data
    tb = paths.regions.harmonize_names(tb)

    # Recalculate World total for pwg variable (exclude regional aggregates)
    # The World total in the source data doesn't match the sum of countries
    regions_to_exclude = [
        "Africa (UN M49)",
        "Americas (UN M49)",
        "Asia (UN M49)",
        "Europe (UN M49)",
        "Oceania (UN M49)",
        "High-income countries",
        "Low-income countries",
        "Lower-middle-income countries",
        "Upper-middle-income countries",
        "World",
    ]

    # Calculate correct world total as sum of all countries (non-region entities)
    countries_only = tb[~tb["country"].isin(regions_to_exclude)]
    world_pwg_corrected = countries_only["pwg"].sum()
    # Update the World row with corrected pwg value
    tb.loc[tb["country"] == "World", "pwg"] = world_pwg_corrected

    # Recalculate per capita values for wg and pwg (convert from daily to annual per capita)
    tb["pwg_per_cap"] = (tb["pwg"] * 1000) / tb["population_2020"]
    tb["wg_per_cap"] = (tb["wg"] * 1000) / tb["population_2020"]

    # Calculate missing per capita variables (convert from tonnes to kg per person)
    per_capita_vars = {
        "plas_litter_em": "plas_litter_em_per_cap",
        "plas_uncol_em": "plas_uncol_em_per_cap",
        "plas_collection_em": "plas_collection_em_per_cap",
        "plas_disp_em": "plas_disp_em_per_cap",
        "plas_recy_em": "plas_recy_em_per_cap",
    }

    for total_var, per_cap_var in per_capita_vars.items():
        if total_var in tb.columns:
            # Convert tonnes to kg (multiply by 1000) and divide by population
            tb[per_cap_var] = (tb[total_var] * 1000) / tb["population_2020"]
            # Copy origins from the source variable
            tb[per_cap_var].metadata.origins = tb[total_var].metadata.origins

    # Calculate share of global total for specified variables
    share_vars = ["plas_em", "plas_burn_em", "plas_debris_em"]

    for var in share_vars:
        if var in tb.columns:
            # Get global total for each year
            global_totals = tb[tb["country"] == "World"].set_index("year")[var]

            # Calculate share (as percentage)
            tb[f"{var}_share_global"] = tb.apply(
                lambda row: (row[var] / global_totals.get(row["year"], float("nan"))) * 100
                if row["country"] != "World" and global_totals.get(row["year"], 0) > 0
                else float("nan"),
                axis=1,
            )
            # Copy origins from the source variable
            tb[f"{var}_share_global"].metadata.origins = tb[var].metadata.origins

    # Drop the population column as it's not needed in the output
    tb = tb.drop(columns=["population_2020"])

    # Set index and format tables
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(
        tables=[tb],
        check_variables_metadata=True,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
