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
    tb = tb[tb["polymer"] == "Total"]
    tb = tb.drop("polymer", axis=1)

    # Aggregate entries for transportation and textile sector
    # Define the mappings
    transportation_entries = ["Transportation - other", "Transportation - tyres"]
    textile_sector_entries = ["Textile sector - others", "Textile sector - clothing"]

    tb = aggregate_entries(tb, transportation_entries, "Transportation")
    tb = aggregate_entries(tb, textile_sector_entries, "Textile sector")

    # Drop the original rows
    entries_to_drop = [
        "Transportation - other",
        "Textile sector - others",
        "Transportation - tyres",
        "Textile sector - clothing",
    ]
    tb = tb[~tb["application"].isin(entries_to_drop)]
    # Replace specific strings in the 'application' column
    tb["application"] = tb["application"].replace(
        {"Consumer & institutional Products": "Consumer & institutional products"}
    )

    # Replace '&' with 'and' in the 'application' column
    tb["application"] = tb["application"].str.replace("&", "and")
    tb["application"] = tb["application"].str.replace("/", " or ")

    total_df = tb.groupby(["year", "application"])["plastic_waste"].sum().reset_index()

    total_df["country"] = "World"
    total_df = total_df.copy_metadata(from_table=tb)

    # Lines below are uncommented for now with the purpose of only using Global estimates
    combined_df = pr.merge(
        total_df, tb, on=["country", "year", "application", "plastic_waste"], how="outer"
    ).copy_metadata(from_table=tb)

    # Drop rows that only have 0 values to avoid indicators with 0t after pivoting
    combined_df = combined_df.loc[combined_df["plastic_waste"] != 0]
    # Drop country column (as it's all going to be global aggregates)
    total_df = total_df.rename(columns={"plastic_waste": "global_value"})

    # Merge the global totals back to the original DataFrame
    df_with_share = pr.merge(combined_df, total_df, on=["year", "application"], how="left")

    # Calculate the share from global total
    df_with_share["share"] = (df_with_share["plastic_waste"] / df_with_share["global_value"]) * 100

    # Optionally, drop the 'Global Value' column if it's not needed
    df_with_share = df_with_share.drop(columns=["global_value", "country_y"])
    df_with_share.rename(columns={"country_x": "country"}, inplace=True)

    tb = df_with_share.underscore().set_index(["country", "year", "application"], verify_integrity=True).sort_index()
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


# Function to aggregate entries
def aggregate_entries(df, entries, new_entry):
    # Filter DataFrame to include only rows with 'application' in entries
    filtered_df = df[df["application"].isin(entries)]

    # Sum the values in the numeric columns and create a new row with 'application' as new_entry
    aggregated_row = filtered_df.groupby(["year", "country"]).sum(numeric_only=True)
    aggregated_row["application"] = new_entry
    aggregated_row = aggregated_row.reset_index()
    # Append the new row to the original DataFrame and drop the aggregated rows
    df = df.append(aggregated_row, ignore_index=True)

    return df
