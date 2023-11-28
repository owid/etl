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
    # Process plastic waste data by polymer type
    tb = by_polymer(tb)
    tb = tb.underscore().set_index(["country", "year", "polymer"], verify_integrity=True).sort_index()
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def by_polymer(tb):
    """
    Aggregate and transform plastic waste data by polymer type.

    This function performs several data cleaning and aggregation tasks on a DataFrame
    that contains plastic waste data, categorized by different polymer types. The
    function first filters the data for entries where 'application' is 'Total', then
    performs grouping and aggregation operations, and finally calculates a new
    column ('share_polymer') that represents the percentage of each entry's
    'plastic_waste' relative to the global total.

    Parameters
    ----------
    tb : pd.DataFrame
        The input data frame which must contain the following columns:
        - 'application'
        - 'polymer'
        - 'plastic_waste'
        - 'year'
    Returns
    -------
    df_with_share : pd.DataFrame
        The transformed data, with 'application' column dropped, string values
        standardized, and an additional column 'share_polymer', which represents
        the percentage of each entry's 'plastic_waste' relative to the global total
        for that year and polymer type. The resulting DataFrame should contain the
        following columns:
        - 'year'
        - 'type' (previously 'polymer')
        - 'plastic_waste_polymer' (previously 'plastic_waste')
        - 'country'
        - 'share_polymer'

    """
    # Filter the data for rows where 'application' is 'Total' and drop the 'application' column
    tb = tb[tb["application"] == "Total"]
    tb = tb.drop("application", axis=1)

    # Group by 'year' and 'polymer', and aggregate 'plastic_waste' using sum
    total_df = tb.groupby(["year", "polymer"])["plastic_waste"].sum().reset_index()

    # Add a 'country' column with the value 'World' and copy metadata from the original DataFrame
    total_df["country"] = "World"
    total_df = total_df.copy_metadata(from_table=tb)

    # Merge the total_df with tb and copy metadata from the original DataFrame
    combined_df = pr.merge(total_df, tb, on=["country", "year", "polymer", "plastic_waste"], how="outer").copy_metadata(
        from_table=tb
    )

    # Remove rows where 'plastic_waste' is 0
    combined_df = combined_df.loc[combined_df["plastic_waste"] != 0]

    # Rename the 'plastic_waste' column in total_df and merge it with combined_df
    total_df = total_df.rename(columns={"plastic_waste": "global_value"})
    df_with_share = pr.merge(combined_df, total_df, on=["year", "polymer"], how="left")

    # Calculate the share of 'plastic_waste' from the 'global_value' and perform final transformations
    df_with_share["share"] = (df_with_share["plastic_waste"] / df_with_share["global_value"]) * 100
    df_with_share = df_with_share.drop(columns=["global_value", "country_y"])
    df_with_share.rename(columns={"country_x": "country"}, inplace=True)

    return df_with_share
