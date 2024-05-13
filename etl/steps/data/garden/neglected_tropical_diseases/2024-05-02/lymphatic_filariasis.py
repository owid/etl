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
    ds_meadow = paths.load_dataset("lymphatic_filariasis")

    # Read table from meadow dataset.
    tb = ds_meadow["lymphatic_filariasis"].reset_index()
    #
    # Harmonize countries
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    # Process data.
    # There are separate rows for each combination of drugs used, but this is duplicate for `national_coverage__pct`, so we will extract this column and create a separate table for it

    tb_nat = tb[["country", "year", "national_coverage__pct"]].copy().drop_duplicates()
    # Drop `national_coverage_pct` from tb
    tb = tb.drop(columns=["national_coverage__pct"])
    # Format the tables
    tb = tb.format(["country", "year", "type_of_mda"])
    tb_nat = tb_nat.format(["country", "year"])
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb, tb_nat], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
