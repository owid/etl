"""Load a snapshot and create a meadow dataset."""

import pandas as pd

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


POP_GROUPS = {
    "All irregular migrants": "All irregular migrants",
    "Non-registered people resident for more than 12 months in Austria": "All irregular migrants",
    "All irregular migrants (including asylum-seekers)": "All irregular migrants (including asylum-seekers)",
    "All irregular migrants (excluding asylum-seekers)": "All irregular migrants",
    "All irregular migrants of non-Schengen nationality": "All irregular migrants",
    "Undocumented migrants": "All irregular migrants",
    "Third-country nationals without required permissions and unknown to authorities": "All irregular migrants",
    "Irregular migrants eligible for regularisation": "Irregular migrants eligible for regularisation",
    "Total undocumented population in the US": "All irregular migrants",
    "Total irregular migrant population": "All irregular migrants",
    "Total unauthorised immigrant population in the US ": "All irregular migrants",
}

COLS_TO_KEEP = [
    "Country",
    "Year",
    "LowEstimate",
    "CentralEstimate",
    "HighEstimate",
    "population",
]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("mirrem_irregular_migration.xlsx")

    # Load data from snapshot.
    tb = snap.read_excel(sheet_name="Database")

    # standardize population group names
    tb = tb[tb["PopulationGroup"].isin(POP_GROUPS.keys())]

    tb["population"] = tb["PopulationGroup"].apply(lambda x: POP_GROUPS[x])

    # remove identical data points
    tb = tb.drop_duplicates(subset=["Country", "Year", "LowEstimate", "CentralEstimate", "HighEstimate"], keep="first")

    # replace timeframes with midpoints
    tb["Year"] = tb["Year"].apply(fix_timeframes)

    # choose one data point per country and year
    tb = remove_duplicates(tb)

    tb.m.short_name = "mirrem_irregular_migration"

    # Keep only the columns we need
    tb = tb.drop(columns=[col for col in tb.columns if col not in COLS_TO_KEEP])

    for col in ["LowEstimate", "CentralEstimate", "HighEstimate"]:
        tb[col] = tb[col].astype(str)

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year", "population"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def remove_duplicates(tb):
    '''Where multiple entries exist for the same country, year, and population group, keep only one.
    This keeps the data entry which represents the total aggregate (incl. asylum seekers) the best or, if that is ambiguous, the entry with the highest "AggregateScore"'''

    duplicates = tb[tb.duplicated(subset=["Country", "Year", "population"], keep=False)]
    countries = duplicates["Country"].unique()
    years = duplicates["Year"].unique()
    for country in countries:
        for year in years:
            these_dupl = duplicates[
                (duplicates["Country"] == country)
                & (duplicates["Year"] == year)
                & (duplicates["population"] == "All irregular migrants")
            ]
            row = these_dupl
            if len(these_dupl) > 0:
                tb = tb.drop(these_dupl.index)
                if country == "Austria":
                    row = these_dupl[
                        these_dupl["PopulationGroup"] == "All irregular migrants (excluding asylum-seekers)"
                    ]
                elif country == "Belgium":
                    row = these_dupl[
                        these_dupl["PopulationGroup"] == "All irregular migrants (excluding asylum-seekers)"
                    ]
                elif country == "Finland":
                    row = these_dupl[these_dupl["PopulationGroup"] == "All irregular migrants"]
                    if len(row) > 1:
                        row = row[
                            row["Source"]
                            == "Asa, R. (2011). Practical measures for reducing irregular migration. Helsinki: EMN & Maahanmuuttovirasto. "
                        ]
                elif country == "Germany":
                    row = these_dupl[
                        these_dupl["PopulationGroup"]
                        == "Third-country nationals without required permissions and unknown to authorities"
                    ]
                elif country == "Italy":
                    row = these_dupl[these_dupl["AggregateScore"] == "High"]
                    if len(row) > 1:
                        row = these_dupl[
                            these_dupl["PopulationGroup"] == "All irregular migrants (excluding asylum-seekers)"
                        ]
                elif country == "Poland":
                    row = these_dupl[these_dupl["Row"] == 159]  # latest data
                elif country == "Spain":
                    row = these_dupl[
                        (
                            (these_dupl["PopulationGroup"] == "All irregular migrants")
                            & (these_dupl["Dataset1"] == "Municipal registry of inhabitants")
                        )
                    ]
                elif country == "United Kingdom":
                    row = these_dupl[these_dupl["PopulationGroup"] == "All irregular migrants"]
                elif country == "United States":
                    row = these_dupl[these_dupl["AggregateScore"] == "High"]
                    if len(row) > 1:
                        row = these_dupl[these_dupl["AggregateNum"] == 11.5]
                    if len(row) == 0:
                        row = these_dupl[these_dupl["AccessScore"] == "High"]
                assert len(row) == 1, f"Multiple rows found for {country} in {year}"
                tb = pd.concat([tb, row]).copy_metadata(tb)
    return tb


def fix_timeframes(tb_year):
    """Where a timeframe is given as a range, replace it with the midpoint of the range"""
    if tb_year in [2000 + i for i in range(25)]:
        return tb_year
    elif "-" in tb_year:
        tb_year_ls = tb_year.split("-")
        tb_year = (int(tb_year_ls[0]) + int(tb_year_ls[1])) / 2
        return tb_year
