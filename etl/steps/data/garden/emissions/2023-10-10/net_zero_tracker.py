"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns to read from main table, and how to rename them.
COLUMNS = {
    "name": "country",
    "end_target_year": "year",
    "end_target_status": "net_zero_status",
    "actor_type": "actor_type",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("net_zero_tracker")
    tb = ds_meadow["net_zero_tracker"].reset_index()

    #
    # Process data.
    #
    # Select and rename columns.
    tb = tb[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Select only rows that correspond to countries.
    tb = tb[tb["actor_type"] == "Country"].drop(columns=["actor_type"]).reset_index(drop=True)

    # Remove rows with incomplete or no data.
    # NOTE: There were a few countries with no data at all, namely Cayman Islands, Libya, and Syria.
    #  Bolivia had status "In policy document" but no year.
    tb = tb.dropna(how="any").reset_index(drop=True)

    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Add a column that simply indicates whether the country has a net zero target.
    # NOTE: All countries in the table have set a net zero target. Those that have not are not in the table (and will
    # show in charts as missing data).
    tb["has_net_zero_target"] = "Net-zero achieved or pledged"
    # Copy metadata from another variable.
    tb["has_net_zero_target"] = tb["has_net_zero_target"].copy_metadata(tb["net_zero_status"])

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
