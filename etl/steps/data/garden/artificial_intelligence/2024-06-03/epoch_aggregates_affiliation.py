"""Load a meadow dataset and create a garden dataset."""
import owid.catalog.processing as pr
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    """
    Generate aggregated table for total yearly and cumulative number of notable AI systems in each category of Researcher affiliation.
    """
    log.info("epoch_aggregates_affiliation.start")

    # Load inputs.
    ds_meadow = paths.load_dataset("epoch")

    # Read table from meadow dataset.
    tb = ds_meadow["epoch"]
    tb = tb.reset_index()

    # Ensure 'publication_date' column type is datetime64
    assert tb["publication_date"].dtype == "datetime64[ns]", "publication_date column is not of type datetime64"

    # Create a 'year' column
    tb["year"] = tb["publication_date"].dt.year

    # Group by 'year' and 'organization_categorization' and calculate counts
    organization_categorization_counts = (
        tb.groupby(["year", "organization_categorization"]).size().reset_index(name="count")
    )

    affiliations = [
        "Academia and government collaboration",
        "Academia and industry collaboration",
        "Industry",
        "Industry and government collaboration",
        "Academia",
        "Industry and research collective collaboration",
        "Not specified",
        "Academia and research collective collaboration",
        "Research collective",
        "Government",
    ]
    # Pivot table for organization types
    df_pivot_org = organization_categorization_counts.pivot(
        index="year", columns="organization_categorization", values="count"
    ).reset_index()
    # Melt dataframe to long format for yearly counts
    melted_df = df_pivot_org.melt(
        id_vars=["year"],
        value_vars=affiliations,
        var_name="affiliation",
        value_name="yearly_count",
    )

    # Replace columns with their cumulative sums
    for column in df_pivot_org.columns:
        if column not in ["year"]:
            df_pivot_org[f"{column}"] = df_pivot_org[column].cumsum()

    # Melt dataframe for cumulative counts
    melted_df_cumulative = df_pivot_org.melt(
        id_vars=["year"],
        value_vars=affiliations,
        var_name="affiliation",
        value_name="cumulative_count",
    )

    # Merge yearly and cumulative counts
    df_merged = pr.merge(melted_df_cumulative, melted_df, on=["year", "affiliation"]).copy_metadata(from_table=tb)

    # Create the aggregated table
    tb_agg = df_merged.underscore().set_index(["year", "affiliation"], verify_integrity=True)

    # Set metadata
    for column in tb_agg:
        tb_agg[column].metadata.origins = tb["organization_categorization"].metadata.origins

    # Save outputs.
    ds_garden = create_dataset(dest_dir, tables=[tb_agg])
    ds_garden.save()

    log.info("epoch_aggregates_affiliation.end")
