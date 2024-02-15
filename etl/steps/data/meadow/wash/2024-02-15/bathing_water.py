"""Load a snapshot and create a meadow dataset."""
import zipfile

from owid.catalog import processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("bathing_water.zip")

    # Load data from snapshot.
    tables = []
    with zipfile.ZipFile(snap.path) as z:
        # open the csv file in the dataset
        with z.open("eea_s_eu-sdg-14-40_p_2000-2022_v01_r00/eu-sdg-14-40_2000-2022_v01_r00.xlsx") as f:
            # read the dataset
            sheet_names = [
                "SDG_14_40_nr_c",
                "SDG_14_40_nr_ex_c",
                "SDG_14_40_%_ex_c",
                "SDG_14_40_nr_in",
                "SDG_14_40_nr_ex_in",
                "SDG_14_40_%_ex_in",
            ]
            for sheet in sheet_names:
                tb_temp = pr.read_excel(
                    f, metadata=snap.to_table_metadata(), origin=snap.m.origin, sheet_name=sheet, skiprows=9
                )
                tb_temp = tb_temp.drop(columns=["Type", "GEO (Codes)"])
                tb_temp = tb_temp.melt(id_vars=["GEO (Labels)"], var_name="year", value_name="value")
                tb_temp = tb_temp.rename(columns={"GEO (Labels)": "country", "value": sheet})
                tb_temp[sheet] = tb_temp[sheet].replace(":", None).astype(float)
                tables.append(tb_temp)

    # Iterate through the remaining dataframes and merge them one by one
    combined_tb = tables[0]

    for tb in tables[1:]:
        combined_tb = pr.merge(combined_tb, tb, how="outer", on=["country", "year"])

    combined_tb.metadata = snap.to_table_metadata()
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[combined_tb], default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
