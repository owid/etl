"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("urban_agglomerations_size_class.xls")

    # Load data from snapshot.
    tb = snap.read()
    #
    # Process data.
    #
    # Find the header row
    header_row_index = None
    for row_idx in range(len(tb)):
        row_values = tb.iloc[row_idx]
        if "Index" in row_values.values:
            header_row_index = row_idx + 1
            break
    # If header row is found, re-read the file with correct header
    if header_row_index is not None:
        tb = snap.read(skiprows=header_row_index)
    # Exclude specified columns from the dataframe if they exist
    columns_to_exclude = ["Index", "Note", "Country Code", "Size class code"]
    # Create a list of columns to keep
    columns_to_keep = [col for col in tb.columns if col not in columns_to_exclude]
    tb = tb[columns_to_keep]

    tb = tb.rename(columns={"Region, subregion, country or area *": "country"})
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.

    tb_melt = tb.melt(id_vars=["country", "Type of data", "Size class of urban settlement"], var_name="year")
    tb_melt = (
        tb_melt.underscore()
        .set_index(["country", "year", "size_class_of_urban_settlement", "type_of_data"], verify_integrity=True)
        .sort_index()
    )
    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir, tables=[tb_melt], check_variables_metadata=True, default_metadata=snap.metadata
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()
