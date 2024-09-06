"""Load a snapshot and create a meadow dataset."""

from owid.catalog import Table
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
    origins = [snap.metadata.origin]

    tb_cont = snap.read(sheet_name="ContinentalTrends")
    tb_cont = format_continental_trends(tb_cont)
    tb_cont = tb_cont.format(["year", "continent"], short_name="continental")

    tb_glob = snap.read(sheet_name="GlobalTrends")
    tb_glob["country"] = "World"
    tb_glob = tb_glob.format(["country", "year"], short_name="global")
    # Adding the origins in
    for col in tb_cont.columns:
        tb_cont[col].metadata.origins = origins

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir, tables=[tb_cont, tb_glob], check_variables_metadata=True, default_metadata=snap.metadata
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def format_continental_trends(tb: Table) -> Table:
    """
    The data is formatted as a wide table, with merged cells in the first row for the taxa, and continents repeated for each taxa in the second row.
    This function extracts this data into a easier to handle format.
    """
    taxa = ["Vascular plants", "Mammals", "Birds", "Fishes", "Insects", "Molluscs"]
    tables = []
    # Grabbing the columns for each taxa separately and then combining them together
    for taxon in taxa:
        col_idx = tb.columns.get_loc(taxon)
        assert isinstance(col_idx, (int)), f"Value is not integer, check spelling of {taxon}"
        # Select the 0th column, the selected taxon, and the next 6 columns
        tb_selected = tb.iloc[:, [0] + list(range(col_idx, col_idx + 6))]
        tb_selected.columns = tb_selected.iloc[0]
        tb_selected = tb_selected.drop(tb_selected.index[0]).reset_index(drop=True)
        tb_selected["taxon"] = taxon
        tables.append(tb_selected)
        # tables = pr.concat([tables, tb_selected])
    tb = pr.concat(tables)
    melted_tb = pr.melt(
        tb,
        id_vars=["Year", "taxon"],
        value_vars=["Africa", "Asia", "Oceania", "Europe", "North America", "South America"],
        var_name="Continent",
        value_name="Count",
    )
    tb = melted_tb.pivot_table(
        index=["Year", "Continent"],
        columns="taxon",
        values="Count",
    ).reset_index()
    return tb
