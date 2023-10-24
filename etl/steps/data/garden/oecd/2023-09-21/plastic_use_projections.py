"""Load a meadow dataset and create a garden dataset."""
import owid.catalog.processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow datasets for global plastic emissions by gas, application type and polymer and read tables.
    ds_meadow = paths.load_dataset("plastic_use_projections")
    tb = ds_meadow["plastic_use_projections"].reset_index()
    #
    # Process data.
    #
    # Convert million to actual number
    tb["plastic_use"] = tb["plastic_use"] * 1e6
    country_mapping_path = paths.directory / "plastic_pollution.countries.json"
    tb = geo.harmonize_countries(df=tb, countries_file=country_mapping_path)

    # Save metadata for later use
    metadata = tb.metadata
    # Create a dictionary to map the original countries/regions to the desired regions
    region_mapping = {
        "Canada": "Americas (excl. USA)",
        "China": "China",
        "India": "India",
        "Latin America": "Americas (excl. USA)",
        "Middle East & North Africa": "Middle East & North Africa",
        "OECD Asia": "Asia (excl. China and India)",
        "OECD European Union": "Europe",
        "OECD Oceania": "Oceania",
        "OECD non-EU": "Europe",
        "Other Africa": "Sub-Saharan Africa",
        "Other EU": "Europe",
        "Other Eurasia": "Asia (excl. China and India)",
        "Other OECD America": "Americas (excl. USA)",
        "Other non-OECD Asia": "Asia (excl. China and India)",
        "United States": "United States",
    }
    # Map the 'country' column to the desired regions using the dictionary
    tb["region"] = tb["country"].map(region_mapping)

    # Drop the 'country' column if it's no longer needed
    tb = tb.drop(columns=["country"])
    tb = tb.rename(columns={"region": "country"})
    # Ensure the regions with the same country name are summed
    tb = tb.groupby(["year", "country"])["plastic_use"].sum().reset_index()
    # Add the metadata back to the table
    tb.metadata = metadata
    # Calculate the global totals
    total_df = tb.groupby("year")["plastic_use"].sum().reset_index()
    total_df["country"] = "World"
    # Merge with the original dataframe
    combined_df = pr.merge(total_df, tb, on=["country", "year", "plastic_use"], how="outer").copy_metadata(
        from_table=tb
    )

    tb = combined_df.underscore().set_index(["country", "year"], verify_integrity=True).sort_index()
    #
    # Save outputs.
    #

    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[tb],
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
