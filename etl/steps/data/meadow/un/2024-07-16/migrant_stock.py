"""Load a snapshot and create a meadow dataset.
Meadow dataset is already very processed to """
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap_dest_origin = paths.load_snapshot("migrant_stock_dest_origin.xlsx")
    snap_origin = paths.load_snapshot("migrant_stock_origin.xlsx")
    snap_destination = paths.load_snapshot("migrant_stock_dest.xlsx")
    snap_sex_age = paths.load_snapshot("migrant_stock_age_sex.xlsx")

    # read and format tables one by one
    # data on destination and origin - table 1
    tb_do = snap_dest_origin.read_excel(sheet_name="Table 1", header=10)

    do_cols_to_drop = [
        "Notes of destination",
        "Location code of destination",
        "Location code of origin",
        "Type of data of destination",
    ]
    do_cols_rename = {
        "Region, development group, country or area of destination": "country_destination",
        "Region, development group, country or area of origin": "country_origin",
    }

    tb_do = drop_and_rename_columns(tb_do, do_cols_to_drop, do_cols_rename)
    tb_do = tb_do.format("index")
    tb_do.metadata.short_name = "migrant_stock_dest_origin"

    # data on destination - table 1 (and table 3
    des_cols_to_drop = ["Notes", "Location code", "Unnamed: 0", "Type of data"]
    cols_rename = {
        "Region, development group, country or area": "country",
    }
    tb_d_total = snap_destination.read_excel(sheet_name="Table 1", header=10)
    tb_d_share = snap_destination.read_excel(sheet_name="Table 3", header=10)

    tb_d_total = drop_and_rename_columns(tb_d_total, des_cols_to_drop, cols_rename)
    tb_d_share = drop_and_rename_columns(tb_d_share, des_cols_to_drop, cols_rename)

    # australia & new zealand shows up twice in the data
    tb_d_total = tb_d_total.drop_duplicates()
    tb_d_share = tb_d_share.drop_duplicates()

    tb_d_total = tb_d_total.format(["country"])
    tb_d_share = tb_d_share.format(["country"])

    tb_d_total.metadata.short_name = "migrant_stock_dest_total"
    tb_d_share.metadata.short_name = "migrant_stock_dest_share"

    # data on origin - table 1
    rest_cols_to_drop = ["Notes", "Location code", "Index", "Type of data"]
    tb_o = snap_origin.read_excel(sheet_name="Table 1", header=10)

    tb_o = drop_and_rename_columns(tb_o, rest_cols_to_drop, cols_rename)

    tb_o = tb_o.drop_duplicates()
    tb_o = tb_o.format(["country"])

    tb_o.metadata.short_name = "migrant_stock_origin"

    # data on age and sex - table 1 (total), table 2 (population per age) and table 4 (share per age group)
    tb_sa_total = snap_sex_age.read_excel(sheet_name="Table 1", header=10)
    tb_pop = snap_sex_age.read_excel(sheet_name="Table 2", header=10)
    tb_sa_share = snap_sex_age.read_excel(sheet_name="Table 4", header=10)

    tb_sa_total = drop_and_rename_columns(tb_sa_total, rest_cols_to_drop, cols_rename)
    tb_sa_share = drop_and_rename_columns(tb_sa_share, rest_cols_to_drop, cols_rename)

    tb_sa_total = tb_sa_total.drop_duplicates()
    tb_sa_share = tb_sa_share.drop_duplicates()

    tb_sa_total = tb_sa_total.format(["country", "year"])
    tb_sa_share = tb_sa_share.format(["country", "year"])

    tb_sa_total.metadata.short_name = "migrant_stock_sex_age_total"
    tb_sa_share.metadata.short_name = "migrant_stock_sex_age_share"

    # including data on total population to calculate comparable rates of migrants in garden step
    tb_pop = tb_pop[["Year", "Region, development group, country or area", "Total", "Total.1", "Total.2"]]
    pop_cols_rename = {
        "Region, development group, country or area": "country",
        "Total": "total_population",
        "Total.1": "male_population",
        "Total.2": "female_population",
    }
    tb_pop = drop_and_rename_columns(tb_pop, [], pop_cols_rename)
    tb_pop = tb_pop.drop_duplicates()
    tb_pop = tb_pop.format(["country", "year"])
    tb_pop.metadata.short_name = "total_population"

    all_tables = [tb_do, tb_d_total, tb_d_share, tb_o, tb_sa_total, tb_sa_share, tb_pop]
    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir, tables=all_tables, check_variables_metadata=True, default_metadata=snap_dest_origin.metadata
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def drop_and_rename_columns(tb, cols_to_drop, cols_rename):
    tb = tb.drop(columns=cols_to_drop)
    tb = tb.rename(columns=cols_rename)
    tb.columns = [str(col) for col in tb.columns]
    for col in tb.columns:
        tb[col] = tb[col].astype(str).str.strip()
    return tb
