"""Load a snapshot and create a meadow dataset."""

from owid.catalog import processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("invasive_species.xlsx")

    # Load data from snapshot.
    tb = snap.read(sheet_name="ContinentalTrends")
    taxa = ["Vascular plants", "Mammals", "Birds", "Fishes", "Insects", "Molluscs"]
    tables = []
    for taxon in taxa:
        col_idx = tb.columns.get_loc(taxon)
        assert isinstance(col_idx, (int)), f"Value is not integer, check spelling of {taxon}"
        # Select the 0th column, the selected taxon, and the next 6 columns
        tb_selected = tb.iloc[:, [0] + list(range(col_idx, col_idx + 5))]
        tb_selected.columns = tb_selected.iloc[0]
        tb_selected = tb_selected.drop(tb_selected.index[0]).reset_index(drop=True)
        tb_selected["taxon"] = taxon
        tables.append(tb_selected)
        # tables = pr.concat([tables, tb_selected])
    tb = pr.concat(tables)
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["Year", "taxon"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
