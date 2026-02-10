"""Load a snapshot and create the World Inequality Dataset meadow dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define missing values (because of the country NA, Namibia)
NA_VALUES = [
    # "-1.#IND",
    # "1.#QNAN",
    # "1.#IND",
    # "-1.#QNAN",
    # "#N/A N/A",
    # "#N/A",
    # "N/A",
    # "n/a",
    # "",
    # "#NA",
    # "NULL",
    # "null",
    # "NaN",
    # "-NaN",
    # "nan",
    # "-nan",
    "",
]

# List of countries/regions not included in the ISO2 standard, but added by WID
# NOTE: I am excluding subnational data from China and "Other" regions, because they have issues
CODES_MISSING = {
    "DD": "East Germany",
    "KS": "Kosovo",
    "QB-PPP": "Africa (WID)",
    "QD-PPP": "Asia (WID)",
    "QE-PPP": "Europe (WID)",
    "QF-PPP": "Oceania (WID)",
    "QJ-PPP": "Central Asia (WID)",
    "QK-PPP": "East Africa (WID)",
    "QL-PPP": "East Asia (WID)",
    "QM-PPP": "Eastern Europe (WID)",
    "QN-PPP": "Middle Africa (WID)",
    "QO-PPP": "North Africa (WID)",
    "QP-PPP": "North America (WID)",
    "QS-PPP": "South-East Asia (WID)",
    "QT-PPP": "South Africa region (WID)",
    "QU-PPP": "South Asia (WID)",
    "QV-PPP": "West Africa (WID)",
    "QW-PPP": "West Asia (WID)",
    "QX-PPP": "Western Europe (WID)",
    "QY-PPP": "European Union (WID)",
    "WO-PPP": "World",
    "XA-PPP": "Asia (excluding Middle East) (WID)",
    "XB-PPP": "North America and Oceania (WID)",
    "XF-PPP": "Sub-Saharan Africa (WID)",
    "XL-PPP": "Latin America (WID)",
    "XM-PPP": "Middle East (WID)",
    "XN-PPP": "MENA (WID)",
    "XR-PPP": "Russia and Central Asia (WID)",
    "XS-PPP": "South & South-East Asia (WID)",
    "ZZ": "Zanzibar",
}

# Market exchange rates regions (we are not using them)
CODES_EXCLUDED = {
    "OA-MER": "Other Russia and Central Asia (at market exchange rate) (WID)",
    "OB-MER": "Other East Asia (at market exchange rate) (WID)",
    "OC-MER": "Other Western Europe (at market exchange rate) (WID)",
    "OD-MER": "Other Latin America (at market exchange rate) (WID)",
    "OE-MER": "Other MENA (at market exchange rate) (WID)",
    "OH-MER": "Other North America and Oceania (at market exchange rate) (WID)",
    "OI-MER": "Other South & South-East Asia (at market exchange rate) (WID)",
    "OJ-MER": "Other Sub-Saharan Africa (at market exchange rate) (WID)",
    "OK-MER": "Other North America (at market exchange rate) (WID)",
    "OL-MER": "Other Oceania (at market exchange rate) (WID)",
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
    "OA-PPP": "Other Russia and Central Asia (WID)",
    "OB-PPP": "Other East Asia (WID)",
    "OC-PPP": "Other Western Europe (WID)",
    "OD-PPP": "Other Latin America (WID)",
    "OE-PPP": "Other MENA (WID)",
    "OH-PPP": "Other North America and Oceania (WID)",
    "OI-PPP": "Other South & South-East Asia (WID)",
    "OJ-PPP": "Other Sub-Saharan Africa (WID)",
    "OK-PPP": "Other North America (WID)",
    "OL-PPP": "Other Oceania (WID)",
    "CN-RU": "China (rural)",
    "CN-UR": "China (urban)",
    "DE-BD": "Baden",
    "DE-BY": "Bavaria",
    "DE-HB": "Bremen",
    "DE-HE": "Hesse",
    "DE-HH": "Hamburg",
    "DE-PR": "Prussia",
    "DE-SN": "Saxony",
    "DE-WU": "Wurttemberg",
    "US-AK": "Alaska",
    "US-AL": "Alabama",
    "US-AR": "Arkansas",
    "US-AZ": "Arizona",
    "US-CA": "California",
    "US-CO": "Colorado",
    "US-CT": "Connecticut",
    "US-DC": "District of Columbia",
    "US-DE": "Delaware",
    "US-FL": "Florida",
    "US-GA": "Georgia",
    "US-HI": "Hawaii",
    "US-IA": "Iowa",
    "US-ID": "Idaho",
    "US-IL": "Illinois",
    "US-IN": "Indiana",
    "US-KS": "Kansas",
    "US-KY": "Kentucky",
    "US-LA": "Louisiana",
    "US-MA": "Massachusetts",
    "US-MD": "Maryland",
    "US-ME": "Maine",
    "US-MI": "Michigan",
    "US-MN": "Minnesota",
    "US-MO": "Missouri",
    "US-MS": "Mississippi",
    "US-MT": "Montana",
    "US-NC": "North Carolina",
    "US-ND": "North Dakota",
    "US-NE": "Nebraska",
    "US-NH": "New Hampshire",
    "US-NJ": "New Jersey",
    "US-NM": "New Mexico",
    "US-NV": "Nevada",
    "US-NY": "New York",
    "US-OH": "Ohio",
    "US-OK": "Oklahoma",
    "US-OR": "Oregon",
    "US-PA": "Pennsylvania",
    "US-RI": "Rhode Island",
    "US-SC": "South Carolina",
    "US-SD": "South Dakota",
    "US-TN": "Tennessee",
    "US-TX": "Texas",
    "US-UT": "Utah",
    "US-VA": "Virginia",
    "US-VT": "Vermont",
    "US-WA": "Washington",
    "US-WI": "Wisconsin",
    "US-WV": "West Virginia",
    "US-WY": "Wyoming",
}


# Create a dictionary with the names of the snapshots and their id variables
SNAPSHOTS_DICT = {
    "world_inequality_database": ["country", "year"],
    "world_inequality_database_distribution": ["country", "year", "welfare", "p", "percentile"],
}


def run() -> None:
    # Keep snapshot info for the main snapshot
    snap_main = paths.load_snapshot("world_inequality_database.csv")

    # Load regions table
    ds_regions = paths.load_dataset("regions")
    tb_regions = ds_regions["regions"].reset_index()

    #
    # Load inputs.

    # Initialize tables list
    tables = []
    for tb_name, tb_ids in SNAPSHOTS_DICT.items():
        # Load data from snapshot.
        # `keep_default_na` and `na_values` are included because there is a country labeled NA, Namibia, which becomes null without the parameters

        # Retrieve snapshot.
        snap = paths.load_snapshot(f"{tb_name}.csv")
        tb = snap.read(keep_default_na=False, na_values=NA_VALUES)

        # Retrieve snapshot with extrapolations
        snap = paths.load_snapshot(f"{tb_name}_with_extrapolations.csv")
        # Load data from snapshot.
        tb_extrapolations = snap.read(keep_default_na=False, na_values=NA_VALUES)

        #
        # Process data.
        #
        # Harmonize countries
        tb = harmonize_countries(
            tb=tb, tb_regions=tb_regions, codes_missing=CODES_MISSING, codes_excluded=CODES_EXCLUDED
        )
        tb_extrapolations = harmonize_countries(
            tb=tb_extrapolations, tb_regions=tb_regions, codes_missing=CODES_MISSING, codes_excluded=CODES_EXCLUDED
        )

        # Set index and sort
        tb = tb.format(tb_ids, short_name=tb_name)
        tb_extrapolations = tb_extrapolations.format(tb_ids, short_name=f"{tb_name}_with_extrapolations")

        # Append current tables
        tables.extend([tb, tb_extrapolations])

    # Add fiscal income data
    snap_fiscal = paths.load_snapshot("world_inequality_database_fiscal.csv")
    tb_fiscal = snap_fiscal.read(keep_default_na=False, na_values=NA_VALUES)

    # Harmonize countries
    tb_fiscal = harmonize_countries(
        tb=tb_fiscal, tb_regions=tb_regions, codes_missing=CODES_MISSING, codes_excluded=CODES_EXCLUDED
    )
    tb_fiscal = tb_fiscal.format(short_name="world_inequality_database_fiscal")

    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(
        tables=tables + [tb_fiscal], check_variables_metadata=True, default_metadata=snap_main.metadata, repack=False
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()


# Country harmonization function, using both the reference country/regional OWID dataset and WID's `iso2_missing` list
def harmonize_countries(tb: Table, tb_regions: Table, codes_missing: dict, codes_excluded: dict) -> Table:
    # Merge dataset and country dictionary to get the name of the country
    tb = pr.merge(tb, tb_regions[["name", "iso_alpha2"]], left_on="country", right_on="iso_alpha2", how="left")

    # Several countries are not matched, because WID amends the ISO-2 list with additional countries and regions
    # See https://wid.world/codes-dictionary/#country-code

    # Make country string to avoid problems with categorical data
    tb["name"] = tb["name"].astype(str)
    tb["country"] = tb["country"].astype(str)

    # Replace missing items
    for x, y in codes_missing.items():
        tb.loc[tb["country"] == x, "name"] = y

    # Create list of unmatched entitites
    missing_list = list(tb[tb["name"] == "nan"]["country"].unique())
    # Substract excluded from missing_list
    missing_list = [x for x in missing_list if x not in codes_excluded.keys()]
    missing_count = len(missing_list)

    # Warns if there are still entities missing
    if missing_count > 0:
        paths.log.warning(
            f"There are still {missing_count} unnamed WID countries/regions in {tb.m.short_name}! Take a look at this list:\n {missing_list}"
        )

    # Drop rows without match (MER if there was not any error)
    tb = tb.loc[~(tb["name"] == "nan"), :].reset_index(drop=True)

    # Drop old country and ISO alpha 2 variable. Rename the newly built variable as `country`
    tb = tb.drop(columns=["country", "iso_alpha2"])
    tb = tb.rename(columns={"name": "country"})

    # Move country and year to the beginning
    cols_to_move = ["country", "year"]
    tb = tb[cols_to_move + [col for col in tb.columns if col not in cols_to_move]]

    return tb
