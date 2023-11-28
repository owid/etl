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
    ds_meadow = paths.load_dataset("average_work_hours")

    # Read table from meadow dataset.
    tb = ds_meadow["average_work_hours"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Remove "Sex:" prefix and extra spaces
    tb["sex"] = tb["sex"].str.replace("Sex:", "", regex=False).str.strip()

    # Remove "Economic activity" prefix and extra spaces
    tb["economic_activity"] = tb["economic_activity"].str.replace("Economic activity", "", regex=False).str.strip()
    # Remove "Education:", "(", and ")" and extra spaces
    tb["education"] = (
        tb["education"]
        .str.replace("Education:", "", regex=False)
        .str.replace("(", "", regex=False)
        .str.replace(")", "", regex=False)
        .str.strip()
    )

    # Filter rows where 'education' contains 'ISCED-11' - most recent classification, and only use both sexes
    filtered_ed = tb[tb["education"].str.contains("Aggregate levels")]
    # filtered_economic_activity = filtered_ed[filtered_ed["economic_activity"].str.contains("(Aggregate)")]
    filtered_sex = filtered_ed[filtered_ed["sex"].str.contains("Total")]

    filtered_sex = filtered_sex.drop(columns="sex")
    filtered_sex = filtered_sex.set_index(
        ["country", "year", "economic_activity", "education"], verify_integrity=True
    ).sort_index()

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[filtered_sex],
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
