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
    ds_meadow = paths.load_dataset("plastic_waste_2019")

    # Read table from meadow dataset.
    tb = ds_meadow["plastic_waste_2019"].reset_index()
    # Convert million to actual number
    tb["plastic_waste"] = tb["plastic_waste"] * 1e6

    #
    # Process data.
    #
    country_mapping_path = paths.directory / "plastic_pollution.countries.json"
    tb = geo.harmonize_countries(df=tb, countries_file=country_mapping_path)
    total_df = tb.groupby(["year", "polymer", "application"])["plastic_waste"].sum().reset_index()

    total_df["country"] = "World"
    total_df = total_df.copy_metadata(from_table=tb)

    # Lines below are uncommented for now with the purpose of only using Global estimates
    # combined_df = pr.merge(
    #    total_df, tb, on=["country", "year", "polymer", "application", "plastic_waste"], how="outer"
    # ).copy_metadata(from_table=tb)

    # Drop rows that only have 0 values to avoid indicators with 0t after pivoting
    total_df = total_df.loc[total_df["plastic_waste"] != 0]
    # Drop country column (as it's all going to be global aggregates)
    tb = tb.drop("country", axis=1)

    # Aggregate entries for transportation and textile sector
    # Define the mappings
    transportation_entries = ["Transportation - other", "Transportation - tyres"]
    textile_sector_entries = ["Textile sector - others", "Textile sector - clothing"]

    total_df = aggregate_entries(total_df, transportation_entries, "Transportation")
    total_df = aggregate_entries(total_df, textile_sector_entries, "Textile sector")
    # Drop the original rows
    entries_to_drop = [
        "Transportation - other",
        "Textile sector - others",
        "Transportation - tyres",
        "Textile sector - clothing",
    ]
    total_df = total_df[~total_df["application"].isin(entries_to_drop)]
    # Replace specific strings in the 'application' column
    total_df["application"] = total_df["application"].replace(
        {"Consumer & institutional Products": "Consumer & institutional products"}
    )

    # Replace '&' with 'and' in the 'application' column
    total_df["application"] = total_df["application"].str.replace("&", "and")

    total_df = total_df.pivot(index=["application", "year"], columns="polymer", values="plastic_waste")
    total_df = total_df.underscore().sort_index()
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[total_df], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


# Function to aggregate entries
def aggregate_entries(df, entries, new_entry):
    # Filter DataFrame to include only rows with 'application' in entries
    filtered_df = df[df["application"].isin(entries)]

    # Sum the values in the numeric columns and create a new row with 'application' as new_entry
    aggregated_row = filtered_df.groupby(["polymer", "year"]).sum(numeric_only=True)
    aggregated_row["application"] = new_entry
    aggregated_row = aggregated_row.reset_index()
    # Append the new row to the original DataFrame and drop the aggregated rows
    df = df.append(aggregated_row, ignore_index=True)

    return df
