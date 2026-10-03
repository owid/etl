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
    ds_meadow = paths.load_dataset("who_glass_by_antibiotic")

    # Read table from meadow dataset.
    tb = ds_meadow["who_glass_by_antibiotic"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    # Split the table into two, one where antibiotic is disregarded (it creates unnecessary duplicates for bcis_per_million and total_bcis)
    tb_bci = (
        tb[["country", "year", "syndrome", "pathogen", "bcis_per_million", "total_bcis"]]
        .drop_duplicates()
        .format(["country", "year", "syndrome", "pathogen"], short_name="bci_table")
    )
    tb_anti = tb[
        [
            "country",
            "year",
            "syndrome",
            "pathogen",
            "antibiotic",
            "bcis_with_ast_per_million",
            "total_bcis_with_ast",
            "share_bcis_with_ast",
        ]
    ].format(["country", "year", "syndrome", "pathogen", "antibiotic"], short_name="antibiotic_table")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb_bci, tb_anti], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
