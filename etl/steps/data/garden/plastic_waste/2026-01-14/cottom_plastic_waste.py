"""Garden step for plastic waste data with country harmonization."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("cottom_plastic_waste")

    # Load population dataset
    ds_population = paths.load_dataset("population")

    # Read tables from meadow dataset.
    tb = ds_meadow.read("cottom_plastic_waste")

    # Read population table
    tb_pop = ds_population.read("population")

    #
    # Process data.
    #
    # Harmonize country names for national data
    tb = paths.regions.harmonize_names(tb)

    # Merge with population data
    tb = pr.merge(tb, tb_pop[["country", "year", "population"]], on=["country", "year"], how="left")

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
            tb[per_cap_var] = (tb[total_var] * 1000) / tb["population"]
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
    tb = tb.drop(columns=["population"])

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
