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
    snap_dest_origin = paths.load_snapshot("un_migrant_stock_dest_origin.xlsx")
    snap_origin = paths.load_snapshot("un_migrant_stock_origin.xlsx")
    snap_destination = paths.load_snapshot("un_migrant_stock_des.xlsx")
    snap_sex_age = paths.load_snapshot("un_migrant_stock_age_sex.xlsx")

    # read and format tables one by one
    # data on destination and origin - table 1
    tb_do = snap_dest_origin.read_excel(sheet_name="Table 1", header=10)

    tb_do = tb_do.drop(
        columns=[
            "Notes of destination",
            "Location code of destination",
            "Location code of origin",
            "Type of data of destination",
        ]
    )
    tb_do = tb_do.rename(
        columns={
            "Region, development group, country or area of destination": "country_destination",
            "Region, development group, country or area of origin": "country_origin",
        }
    )
    tb_do.columns = [str(col) for col in tb_do.columns]
    tb_do = tb_do.format(["index"])

    # data on destination - table 1 (and table 3
    cols_to_drop = ["Notes", "Location code", "Unnamed: 0", "Type of data"]
    cols_rename = {
        "Region, development group, country or area": "country",
    }
    tb_d_total = snap_destination.read_excel(sheet_name="Table 1", header=10)
    tb_d_share = snap_destination.read_excel(sheet_name="Table 3", header=10)

    tb_d_total = tb_d_total.drop(columns=cols_to_drop)
    tb_d_total = tb_d_total.rename(columns=cols_rename)

    tb_d_share = tb_d_share.drop(columns=cols_to_drop)
    tb_d_share = tb_d_share.rename(columns=cols_rename)

    tb_d_total = tb_do.format(["country"])
    tb_d_share = tb_do.format(["country"])

    # data on origin - table 1
    tb_o = snap_origin.read_excel(sheet_name="Table 1", header=10)

    tb_o = tb_o.drop(columns=cols_to_drop)
    tb_o = tb_o.rename(columns=cols_rename)

    # data on age and sex - table 1 (total) and table 4 (share per age group)
    tb_sa_total = snap_sex_age.read_excel(sheet_name="Table 1", header=10)
    tb_sa_share = snap_sex_age.read_excel(sheet_name="Table 4", header=10)

    tb_sa_total = tb_sa_total.drop(columns=cols_to_drop)
    tb_sa_total = tb_sa_total.rename(columns=cols_rename)

    tb_sa_share = tb_sa_share.drop(columns=cols_to_drop)
    tb_sa_share = tb_sa_share.rename(columns=cols_rename)

    tb_sa_total = tb_sa_total.format(["country", "year"])
    tb_sa_share = tb_sa_share.format(["country", "year"])

    all_tables = [tb_do, tb_d_total, tb_d_share, tb_o, tb_sa_total, tb_sa_share]
    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir, tables=all_tables, check_variables_metadata=True, default_metadata=snap_dest_origin.metadata
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()
