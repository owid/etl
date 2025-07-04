"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    snapshot_names = ["affordable_housing_income.xlsx", "affordable_housing.xlsx"]
    tables = []

    snap_1_1 = paths.load_snapshot("affordable_housing_income.xlsx")
    snap_1_2 = paths.load_snapshot("affordable_housing.xlsx")


    tb_share_of_consumption = snap_1_1.read_excel(sheet_name="Figure HC1.1.2", skipcolumns=9, header=3, na_values=["-"])
    tb_hc_burden = snap_1_2.read_excel(sheet_name="HC12_A1", header=5) #split by owner and renter
    tb_hc_burden_avg = snap_1_2.read_excel(sheet_name="HC12_A1_a", header=5) #average across owner and renter
    tb_hc_burden_quintile = snap_1_2.read_excel(sheet_name="HC12_A2", header=[5, 6]) #split by quintile
    tb_hc_overburden_quintile = snap_1_2.read_excel(sheet_name="HC12_A3", header=[5, 6]) #split by owner and renter, each quintile
    tb_hc_overburden = snap_1_2.read_excel(sheet_name="HC12_A3_a", header=5) #split by owner and renter
    tb_hc_overburden_low_income = snap_1_2.read_excel(sheet_name="HC12_A3_b", header=5) #bottom quintile, average across owner and renter
    tb_hc_overburden_avg = snap_1_2.read_excel(sheet_name="HC12_A3_c", header=4) #average across nation

    




    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables)

    # Save meadow dataset.
    ds_meadow.save()
