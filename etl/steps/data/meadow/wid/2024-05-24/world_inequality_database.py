"""Load a snapshot and create the World Inequality Dataset meadow dataset."""


import owid.catalog.processing as pr
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# List of countries/regions not included in the ISO2 standard, but added by WID
iso2_missing = {
    "CN-RU": "China (rural)",
    "CN-UR": "China (urban)",
    "DD": "East Germany",
    "KS": "Kosovo",
    "OA": "Other Russia and Central Asia (WID)",
    "OB": "Other East Asia (WID)",
    "OC": "Other Western Europe (WID)",
    "OD": "Other Latin America (WID)",
    "OE": "Other MENA (WID)",
    "OH": "Other North America and Oceania (WID)",
    "OI": "Other South & South-East Asia (WID)",
    "OJ": "Other Sub-Saharan Africa (WID)",
    "QB": "Africa (WID)",
    "QD": "Asia (WID)",
    "QE": "Europe (WID)",
    "QF": "Oceania (WID)",
    "QJ": "Central Asia (WID)",
    "QK": "East Africa (WID)",
    "QL": "East Asia (WID)",
    "QM": "Eastern Europe (WID)",
    "QN": "Middle Africa (WID)",
    "QO": "North Africa (WID)",
    "QP": "North America (WID)",
    "QS": "South-East Asia (WID)",
    "QT": "South Africa region (WID)",
    "QU": "South Asia (WID)",
    "QV": "West Africa (WID)",
    "QW": "West Asia (WID)",
    "QX": "Western Europe (WID)",
    "QY": "European Union (WID)",
    "WO": "World",
    "XA": "Asia (excluding Middle East) (WID)",
    "XB": "North America and Oceania (WID)",
    "XF": "Sub-Saharan Africa (WID)",
    "XL": "Latin America (WID)",
    "XM": "Middle East (WID)",
    "XN": "MENA (WID)",
    "XR": "Russia and Central Asia (WID)",
    "XS": "South & South-East Asia (WID)",
    "ZZ": "Zanzibar",
}

# Market exchange rates regions (we are not using them)
iso2_missing_mer = {
    "OA-MER": "Other Russia and Central Asia (at market exchange rate) (WID)",
    "OB-MER": "Other East Asia (at market exchange rate) (WID)",
    "OC-MER": "Other Western Europe (at market exchange rate) (WID)",
    "OD-MER": "Other Latin America (at market exchange rate) (WID)",
    "OE-MER": "Other MENA (at market exchange rate) (WID)",
    "OH-MER": "Other North America and Oceania (at market exchange rate) (WID)",
    "OI-MER": "Other South & South-East Asia (at market exchange rate) (WID)",
    "OJ-MER": "Other Sub-Saharan Africa (at market exchange rate) (WID)",
    "QB-MER": "Africa (at market exchange rate) (WID)",
    "QD-MER": "Asia (at market exchange rate) (WID)",
    "QE-MER": "Europe (at market exchange rate) (WID)",
    "QF-MER": "Oceania (at market exchange rate) (WID)",
    "QJ-MER": "Central Asia (at market exchange rate) (WID)",
    "QK-MER": "East Africa (at market exchange rate) (WID)",
    "QL-MER": "East Asia (at market exchange rate) (WID)",
    "QM-MER": "Eastern Europe (at market exchange rate) (WID)",
    "QN-MER": "Middle Africa (at market exchange rate) (WID)",
    "QO-MER": "North Africa (at market exchange rate) (WID)",
    "QP-MER": "North America (at market exchange rate) (WID)",
    "QS-MER": "South-East Asia (at market exchange rate) (WID)",
    "QT-MER": "South Africa region (at market exchange rate) (WID)",
    "QU-MER": "South Asia (at market exchange rate) (WID)",
    "QV-MER": "West Africa (at market exchange rate) (WID)",
    "QW-MER": "West Asia (at market exchange rate) (WID)",
    "QX-MER": "Western Europe (at market exchange rate) (WID)",
    "QY-MER": "European Union (at market exchange rate) (WID)",
    "WO-MER": "World (at market exchange rate) (WID)",
    "XA-MER": "Asia (excluding Middle East) (at market exchange rate) (WID)",
    "XB-MER": "North America and Oceania (at market exchange rate) (WID)",
    "XF-MER": "Sub-Saharan Africa (at market exchange rate) (WID)",
    "XL-MER": "Latin America (at market exchange rate) (WID)",
    "XM-MER": "Middle East (at market exchange rate) (WID)",
    "XN-MER": "MENA (at market exchange rate) (WID)",
    "XR-MER": "Russia and Central Asia (at market exchange rate) (WID)",
    "XS-MER": "South & South-East Asia (at market exchange rate) (WID)",
}

# Create a dictionary with the names of the snapshots and their id variables
snapshots_dict = {
    "world_inequality_database": ["country", "year"],
    "world_inequality_database_distribution": ["country", "year", "welfare", "p", "percentile"],
}


def run(dest_dir: str) -> None:
    # Keep snapshot info for the main snapshot
    snap_main = paths.load_snapshot("world_inequality_database.csv")

    # Load regions table
    ds_regions = paths.load_dataset("regions")
    tb_regions = ds_regions["regions"].reset_index()

    #
    # Load inputs.

    # Initialize tables list
    tables = []
    for tb_name, tb_ids in snapshots_dict.items():
        # Load data from snapshot.
        # `keep_default_na` and `na_values` are included because there is a country labeled NA, Namibia, which becomes null without the parameters
        na_values = [
            "-1.#IND",
            "1.#QNAN",
            "1.#IND",
            "-1.#QNAN",
            "#N/A N/A",
            "#N/A",
            "N/A",
            "n/a",
            "",
            "#NA",
            "NULL",
            "null",
            "NaN",
            "-NaN",
            "nan",
            "-nan",
            "",
        ]

        # Retrieve snapshot.
        snap = paths.load_snapshot(f"{tb_name}.csv")
        tb = snap.read(keep_default_na=False, na_values=na_values)

        # Retrieve snapshot with extrapolations
        snap = paths.load_snapshot(f"{tb_name}_with_extrapolations.csv")
        # Load data from snapshot.
        tb_extrapolations = snap.read(keep_default_na=False, na_values=na_values)

        # Combine both datasets
        tb = pr.merge(tb, tb_extrapolations, on=tb_ids, how="outer", suffixes=("", "_extrapolated"), short_name=tb_name)

        #
        # Process data.
        #
        # Harmonize countries
        tb = harmonize_countries(tb, tb_regions, iso2_missing, iso2_missing_mer)

        # Set index and sort
        tb = tb.format(tb_ids)

        # Append current table
        tables.append(tb)

    # Add fiscal income data
    snap_fiscal = paths.load_snapshot("world_inequality_database_fiscal.csv")
    tb_fiscal = snap_fiscal.read(keep_default_na=False, na_values=na_values)

    # Harmonize countries
    tb_fiscal = harmonize_countries(tb_fiscal, tb_regions, iso2_missing, iso2_missing_mer)
    tb_fiscal.metadata.short_name = "world_inequality_database_fiscal"
    tb_fiscal = tb_fiscal.format()

    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir, tables=tables + [tb_fiscal], check_variables_metadata=True, default_metadata=snap_main.metadata
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()


# Country harmonization function, using both the reference country/regional OWID dataset and WID's `iso2_missing` list
def harmonize_countries(tb: Table, tb_regions: Table, iso2_missing: dict, iso_2_missing_mer: dict) -> Table:
    # Merge dataset and country dictionary to get the name of the country
    tb = pr.merge(tb, tb_regions[["name", "iso_alpha2"]], left_on="country", right_on="iso_alpha2", how="left")

    # Several countries are not matched, because WID amends the ISO-2 list with additional countries and regions
    # See https://wid.world/codes-dictionary/#country-code

    # Make country string to avoid problems with categorical data
    tb["name"] = tb["name"].astype(str)
    tb["country"] = tb["country"].astype(str)

    # Replace missing items
    for x, y in iso2_missing.items():
        tb.loc[tb["country"] == x, "name"] = y

    # Create list of unmatched entitites
    missing_list = list(tb[tb["name"] == "nan"]["country"].unique())
    # Substract iso2_missing_mer from missing_list
    missing_list = [x for x in missing_list if x not in iso_2_missing_mer.keys()]
    missing_count = len(missing_list)

    # Warns if there are still entities missing
    if missing_count > 0:
        log.warning(
            f"There are still {missing_count} unnamed WID countries/regions in {tb.m.short_name}! Take a look at this list:\n {missing_list}"
        )

    # Drop rows without match (MER if there was not any error)
    tb = tb[~(tb["name"] == "nan")].reset_index(drop=True)

    # Drop old country and ISO alpha 2 variable. Rename the newly built variable as `country`
    tb = tb.drop(columns=["country", "iso_alpha2"])
    tb = tb.rename(columns={"name": "country"})

    # Move country and year to the beginning
    cols_to_move = ["country", "year"]
    tb = tb[cols_to_move + [col for col in tb.columns if col not in cols_to_move]]

    return tb
