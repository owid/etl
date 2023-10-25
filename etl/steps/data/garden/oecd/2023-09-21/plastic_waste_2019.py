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
    tb = tb.groupby(["year", "polymer", "application", "country"])["plastic_waste"].sum().reset_index()
    # Add the metadata back to the table
    tb.metadata = metadata
    # Process plastic waste data by application type
    tb = by_application(tb)
    tb = tb.underscore().set_index(["country", "year", "application"], verify_integrity=True).sort_index()
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def by_application(tb):
    """
    Aggregate and transform plastic waste data by application type.

    This function performs several data cleaning and aggregation tasks on a DataFrame
    that contains plastic waste data, categorized by different application types. The
    function first aggregates certain application types into broader categories, then
    performs string replacements to standardize names, and finally calculates a new
    column ('share') that represents the percentage of each entry's 'plastic_waste'
    relative to thxfe global total.

    Parameters
    ----------
    tb : pd.DataFrame
        The input data frame which must contain the following:
        - 'polymer'
        - 'application'
        - 'plastic_waste'
        - 'year'

        The 'polymer' column should include an entry called 'Total'.

    Returns
    -------
    df_with_share : pd.DataFrame
        The transformed data, with several application types aggregated, string values
        standardized, and an additional column 'share', which represents the percentage
        of each entry's 'plastic_waste' relative to the global total for that year and
        application type. The resulting DataFrame should contain the following columns:
        - 'year'
        - 'type' (previously 'application')
        - 'plastic_waste'
        - 'country'
        - 'share'

    """
    tb = tb[tb["polymer"] == "Total"]
    tb = tb.drop("polymer", axis=1)

    # Aggregate entries for transportation and textile sector
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

    # Replace '&' with 'and' and '/' with ' or ' in the 'application' column
    tb["application"] = tb["application"].str.replace("&", "and")
    tb["application"] = tb["application"].str.replace("/", " or ")

    # Aggregate the plastic_waste by year and application and create a 'country' column
    total_df = tb.groupby(["year", "application"])["plastic_waste"].sum().reset_index()
    total_df["country"] = "World"
    total_df = total_df.copy_metadata(from_table=tb)

    # Merge the total_df with tb and perform additional transformations
    combined_df = pr.merge(
        total_df, tb, on=["country", "year", "application", "plastic_waste"], how="outer"
    ).copy_metadata(from_table=tb)
    combined_df = combined_df.loc[combined_df["plastic_waste"] != 0]

    # Rename the 'plastic_waste' column in total_df and merge it with combined_df
    total_df = total_df.rename(columns={"plastic_waste": "global_value"})
    df_with_share = pr.merge(combined_df, total_df, on=["year", "application"], how="left")

    # Calculate the share from global total and perform final transformations
    df_with_share["share"] = (df_with_share["plastic_waste"] / df_with_share["global_value"]) * 100
    df_with_share = df_with_share.drop(columns=["global_value", "country_y"])
    df_with_share.rename(columns={"country_x": "country"}, inplace=True)

    return df_with_share


def aggregate_entries(df, entries, new_entry):
    """
    Aggregate specific entries in a DataFrame and append the aggregated data.

    Given a DataFrame, a list of entries, and a new entry name, this function
    filters the DataFrame for rows where 'application' is in the provided list
    of entries. It then aggregates (sums) the numeric values in these rows,
    assigns the specified new entry name to 'application', and appends this
    aggregated data back to the original DataFrame. The rows used for
    aggregation are not removed from the original DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        The input data frame which must contain the following columns:
        - 'application'
        - 'year'
        - 'country'

        The 'application' column should include entries that match those
        specified in the 'entries' parameter.

    entries : list of str
        List of entry names in the 'application' column that should be
        aggregated.

    new_entry : str
        The name to assign to the 'application' column in the aggregated row.

    Returns
    -------
    df : pd.DataFrame
        The original DataFrame with the aggregated data appended as additional
        rows. The resulting DataFrame will contain the same columns as the
        input DataFrame.
    """
    # Filter DataFrame to include only rows with 'application' in entries
    filtered_df = df[df["application"].isin(entries)]

    # Sum the values in the numeric columns and create a new row with 'application' as new_entry
    aggregated_row = filtered_df.groupby(["year", "country"]).sum(numeric_only=True)
    aggregated_row["application"] = new_entry
    aggregated_row = aggregated_row.reset_index()

    # Append the new row to the original DataFrame
    df = pr.concat([df, aggregated_row], ignore_index=True)

    return df
