"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    # Load inputs.
    #
    # Retrieve snapshot.
    snap_dest_origin = paths.load_snapshot("migrant_stock_dest_origin.xlsx")
    snap_origin = paths.load_snapshot("migrant_stock_origin.xlsx")
    snap_destination = paths.load_snapshot("migrant_stock_dest.xlsx")

    ## read and format tables one by one
    # data on destination and origin - table 1
    tb_do = snap_dest_origin.read_excel(sheet_name="Table 1", header=10)

    drop_cols_do = ["Location code of destination", "Location code of origin", "Coverage", "Data type"]

    do_cols_rename = {
        "Region, development group, country or area of destination": "country_destination",
        "Region, development group, country or area of origin": "country_origin",
    }

    # drop columns and rename them
    tb_do = drop_and_rename_columns(tb_do, drop_cols_do, do_cols_rename)
    tb_do = tb_do.format("index", short_name="migrant_stock_dest_origin")

    # data on destination - table 1 (total number of migrants) and table 3 (share of migrants)
    drop_cols_d = ["Location code", "Unnamed: 0", "Coverage", "Data type"]
    cols_rename = {
        "Region, development group, country or area": "country",
    }
    tb_d_total = snap_destination.read_excel(sheet_name="Table 1", header=10, na_values=[".."])
    tb_d_pop = snap_destination.read_excel(sheet_name="Table 2", header=10, na_values=[".."])
    tb_d_share = snap_destination.read_excel(sheet_name="Table 3", header=10, na_values=[".."])

    tb_d_total = drop_and_rename_columns(tb_d_total, drop_cols_d, cols_rename)
    tb_d_pop = drop_and_rename_columns(
        tb_d_pop, cols_to_drop=["Unnamed: 0", "Population notes", "Location code"], cols_rename=cols_rename
    )
    tb_d_share = drop_and_rename_columns(tb_d_share, drop_cols_d, cols_rename)

    # australia & new zealand shows up twice in the data
    tb_d_total = tb_d_total.drop_duplicates()
    tb_d_pop = tb_d_pop.drop_duplicates()
    tb_d_share = tb_d_share.drop_duplicates()

    tb_d_total = tb_d_total.format(["country"], short_name="migrant_stock_dest_total")
    tb_d_pop = tb_d_pop.format(["country"], short_name="un_desa_total_population")
    tb_d_share = tb_d_share.format(["country"], short_name="migrant_stock_dest_share")

    # data on origin - table 1
    drop_cols_o = ["Location code", "Index", "Coverage", "Unnamed: 3"]
    tb_o = snap_origin.read_excel(sheet_name="Table 1", header=10, na_values=[".."])

    tb_o = drop_and_rename_columns(tb_o, drop_cols_o, cols_rename)

    tb_o = tb_o.drop_duplicates()
    tb_o = tb_o.format(["country"], short_name="migrant_stock_origin")

    all_tables = [tb_do, tb_d_total, tb_d_share, tb_o, tb_d_pop]

    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=all_tables)

    # Save meadow dataset.

    ds_meadow.save()


# helper function to drop and rename columns, including converting to string and cleaning column names
def drop_and_rename_columns(tb, cols_to_drop, cols_rename):
    tb = tb.drop(columns=cols_to_drop, errors="raise")
    tb = tb.rename(columns=cols_rename, errors="raise")
    tb.columns = [str(col) for col in tb.columns]
    for col in tb.columns:
        tb[col] = tb[col].astype(str).str.strip()
    return tb
