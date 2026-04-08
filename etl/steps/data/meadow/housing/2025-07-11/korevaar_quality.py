"""Load a snapshot and create a meadow dataset."""

from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

AMS_RENAME = {
    "Year": "year",
    "City": "city",
    "Rooms per person": "rooms_per_occupant",
    "surface in m2": "surface_m2",
    "m2perpersonnewdefintion": "surface_per_occupant",
    "Persons per house": "occupants",
    "% inside toilet": "inside_toilet_pct",
    "% with bathroom": "bathroom_pct",
    "% c.v.": "central_heating_pct",
    "%water": "water_pct",
    "%electricity": "electricity_pct",
}

PAR_RENAME = {
    "Unnamed: 1": "city",
    "Unnamed: 2": "year",
    "Persons per home": "occupants",
    "Rooms per home": "rooms",
    "Rooms per person": "rooms_per_occupant",
    "% water ": "water_pct",
    "% bath room": "bathroom_pct",
    "% own toilet": "inside_toilet_pct",
    "% central hetaing": "central_heating_pct",
}

LON_RENAME = {"Unnamed: 1": "year", "Rooms per capita": "rooms_per_capita"}

BEL_RENAME = {
    "Year": "year",
    "City": "city",
    "R/Capita": "rooms_per_capita",
    "R/Occupant": "rooms_per_occupant",
    "%water": "water_pct",
    "%toilets": "inside_toilet_pct",
    "%bathroom": "bathroom_pct",
    "%heating": "central_heating_pct",
    "%kitchen>4m2": "kitchen_greater_than_4m2_pct",
    "%isolation": "isolation_pct",
    "%gas": "gas_pct",
}


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("korevaar_quality.xlsx")

    # Load data from snapshot.
    tb_ams = snap.read_excel(sheet_name="Amsterdam")
    tb_par = snap.read_excel(sheet_name="Paris")
    tb_lon = snap.read_excel(sheet_name="London")
    tb_bel = snap.read_excel(sheet_name="Belgian cities")

    # Rename columns for consistency.
    tb_ams = tb_ams[AMS_RENAME.keys()].rename(columns=AMS_RENAME)
    tb_par = tb_par[PAR_RENAME.keys()].rename(columns=PAR_RENAME)
    tb_lon = tb_lon[LON_RENAME.keys()].rename(columns=LON_RENAME)
    tb_bel = tb_bel[BEL_RENAME.keys()].rename(columns=BEL_RENAME)

    tb_par = tb_par.replace({"city": {"Paris (75)": "Paris"}})
    tb_lon["city"] = "London"
    tb_bel = tb_bel.replace({"city": {"Gent": "Ghent"}})

    # give percentages as percentage
    for tb in [tb_par, tb_lon, tb_bel, tb_ams]:
        for col in tb.columns:
            if col.endswith("_pct"):
                tb[col] = tb[col].astype(float)
                # check whether all entries are smaller than 1
                if tb[col].max() < 1:
                    # if so, multiply by 100
                    tb[col] = tb[col] * 100
                # assert that all entries are between 0 and 100
                assert (tb[col].min() >= 0) and (
                    tb[col].max() <= 100
                ), f"Column {col} has values outside of [0, 100] range."

    tb_all = pr.concat([tb_ams, tb_par, tb_lon, tb_bel], ignore_index=True)  # type: ignore

    #
    # Process data.
    #
    # Improve tables format.
    tables = [tb_all.format(["city", "year"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
