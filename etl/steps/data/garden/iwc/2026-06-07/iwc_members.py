"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table
from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# Define periods of gaps and leaving for countries that have withdrawn and rejoined or left without rejoining.
# Source: https://www.state.gov/wp-content/uploads/2024/05/Intl-Convention-for-the-Regulation-of-Whaling.pdf (from 2019)
GAPS_AND_LEFT = [
    {"country": "Netherlands", "withdrawal_year": 1959, "rejoining_year": 1962},
    {"country": "Norway", "withdrawal_year": 1959, "rejoining_year": 1960},
    {"country": "Sweden", "withdrawal_year": 1964, "rejoining_year": 1979},
    {"country": "Brazil", "withdrawal_year": 1966, "rejoining_year": 1974},
    {"country": "New Zealand", "withdrawal_year": 1969, "rejoining_year": 1976},
    {"country": "Netherlands", "withdrawal_year": 1970, "rejoining_year": 1977},
    {"country": "Panama", "withdrawal_year": 1980, "rejoining_year": 2001},
    {"country": "Dominica", "withdrawal_year": 1983, "rejoining_year": 1992},
    {"country": "Belize", "withdrawal_year": 1988, "rejoining_year": 2003},
    {"country": "Solomon Islands", "withdrawal_year": 1990, "rejoining_year": 1993},
    {"country": "Uruguay", "withdrawal_year": 1991, "rejoining_year": 2007},
    {"country": "Iceland", "withdrawal_year": 1992, "rejoining_year": 2002},
    # left without rejoining
    {"country": "Canada", "withdrawal_year": 1982, "rejoining_year": None},
    {"country": "Jamaica", "withdrawal_year": 1984, "rejoining_year": None},
    {"country": "Egypt", "withdrawal_year": 1989, "rejoining_year": None},
    {"country": "Mauritius", "withdrawal_year": 1988, "rejoining_year": None},
    {"country": "Philippines", "withdrawal_year": 1988, "rejoining_year": None},
    {"country": "Seychelles", "withdrawal_year": 1995, "rejoining_year": None},
    {"country": "Venezuela", "withdrawal_year": 1999, "rejoining_year": None},
    {"country": "Greece", "withdrawal_year": 2013, "rejoining_year": None},
    {"country": "Guatemala", "withdrawal_year": 2017, "rejoining_year": None},
    {"country": "Japan", "withdrawal_year": 2019, "rejoining_year": None},
]


def adherence_to_joined_year(tb):
    """Add a column with the year of joining the IWC.
    The adherence day is given as dd/mm/yy, so we extract the year from it. Years >48 are in the 1900s, while years <=48 are in the 2000s.

    For countries who have withdrawn the year should be the original joining year. These are:

    - Netherlands: 1948
    - Norway: 1948
    - Sweden: 1949
    - Brazil: 1950
    - New Zealand: 1949
    - Panama: 1948
    - Dominica: 1981
    - Belize: 1982
    - Solomon Islands: 1985
    - Uruguay: 1981
    - Iceland: 1947
    - Ecuador: 1991
    """
    og_joining_years = {
        "Netherlands": 1948,
        "Norway": 1948,
        "Sweden": 1949,
        "Brazil": 1950,
        "New Zealand": 1949,
        "Panama": 1948,
        "Dominica": 1981,
        "Belize": 1982,
        "Solomon Islands": 1985,
        "Uruguay": 1981,
        "Iceland": 1947,
        "Ecuador": 1991,
    }
    tb["year_joined"] = None
    for _, row in tb.iterrows():
        adherence_day = row["adherence"]
        if adherence_day is not None:
            day, month, year = adherence_day.split("/")
            year = int(year)
            if year >= 30:
                year += 1900
                if year < 1948:
                    # some countries signed before 1948, but the IWC was established in 1948, so we set the joining year to 1948 for these countries
                    year = 1948
            else:
                year += 2000
            if row["country"] in og_joining_years:
                year = og_joining_years[row["country"]]
            tb.at[_, "year_joined"] = year
    return tb


def add_former_members(tb):
    left_countries = [
        {"country": "Canada", "year_joined": 1949},
        {"country": "Jamaica", "year_joined": 1981},
        {"country": "Egypt", "year_joined": 1981},
        {"country": "Mauritius", "year_joined": 1983},
        {"country": "Philippines", "year_joined": 1981},
        {"country": "Seychelles", "year_joined": 1979},
        {"country": "Venezuela", "year_joined": 1991},
        {"country": "Japan", "year_joined": 1951},
        {"country": "Greece", "year_joined": 2007},
        {"country": "Guatemala", "year_joined": 2006},
    ]
    tb = pr.concat(
        [tb, Table(left_countries)],
        ignore_index=True,
    )

    return tb


def add_gaps_and_left_periods(tb):
    """
    Update tb with the gaps and leaving periods.

    For these periods countries are set as non-members and former members.

    For each gap/subsequent rejoining, the year of joining is the last joining year and the year of leaving is the last leaving year.
    For example, for Netherlands, the year of joining is 1948 between 1948 and 1962 and 1962 between 1962 and 1977.

    The year of leaving is 1959 for 1959-1962 and 1970 for all years between 1970 and 1977.
    """
    for gap in GAPS_AND_LEFT:
        country = gap["country"]
        withdrawal_year = gap["withdrawal_year"]
        rejoining_year = gap["rejoining_year"]

        if rejoining_year is not None:
            # Add gap period
            gap_msk = (tb["country"] == country) & (tb["year"] >= withdrawal_year) & (tb["year"] < rejoining_year)
            rejoined_msk = (tb["country"] == country) & (tb["year"] >= rejoining_year)

            # set member and former member status for gap period
            tb.loc[gap_msk, "member"] = False
            tb.loc[gap_msk, "former_member"] = True
            tb.loc[gap_msk, "year_left"] = withdrawal_year

            # set new joining year for rejoining period
            tb.loc[rejoined_msk, "year_joined"] = rejoining_year
            tb.loc[rejoined_msk, "year_left"] = None  # clear any permanent leaving year for rejoining period
        else:
            # Add leaving year for countries that left without rejoining
            left_msk = (tb["country"] == country) & (tb["year"] >= withdrawal_year)
            tb.loc[left_msk, "member"] = False
            tb.loc[left_msk, "former_member"] = True
            tb.loc[left_msk, "year_left"] = withdrawal_year
    return tb


def create_member_tb(tb):
    """Create a table with one row per country and year, indicating whether the country was a member of the IWC in that year."""
    tb_rows = []
    for year in range(1948, 2026):
        for _, row in tb.iterrows():
            country = row["country"]
            year_joined = row["year_joined"]
            member = False
            if year_joined is not None and year_joined <= year:
                member = True
            tb_rows.append(
                {
                    "country": country,
                    "year": year,
                    "member": member,
                    "former_member": False,
                    "year_joined": year_joined if member else None,
                    "year_left": None,
                }
            )

    return Table(tb_rows)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("iwc_members")

    # Read table from meadow dataset.
    tb = ds_meadow.read("iwc_members")

    # drop commissioner and appointment columns
    tb = tb.drop(columns=["commissioner", "appointment"])

    tb = tb.rename(columns={"contracting_government": "country"})

    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb=tb)

    # Add year of joining column.
    tb = adherence_to_joined_year(tb)

    # drop adherence column
    tb = tb.drop(columns=["adherence"])

    # add former members that are not in the original table (countries that left without rejoining)
    tb = add_former_members(tb)

    # Add year_left (will be filled in add_gaps_and_left_periods)
    tb["year_left"] = None

    tb_metadata = tb.metadata
    tb_origins = tb["country"].m.origins

    # Create member table.
    tb_members = create_member_tb(tb)

    # Add gaps and leaving periods.
    tb_members = add_gaps_and_left_periods(tb_members)

    # add member/ former member column
    tb_members["current_status"] = tb_members.apply(
        lambda row: "Member" if row["member"] else ("Former member" if row["former_member"] else "Not a member"), axis=1
    )

    # make years integers
    tb_members["year"] = tb_members["year"].astype(int)
    tb_members["year_joined"] = tb_members["year_joined"].astype("Int64")
    tb_members["year_left"] = tb_members["year_left"].astype("Int64")

    tb_members.metadata = tb_metadata
    for col in tb_members.columns:
        tb_members[col].m.origins = tb_origins

    # add greenland with same data as denmark (as it is a member of the IWC through Denmark)
    greenland_tb = tb_members[tb_members["country"] == "Denmark"].copy()
    greenland_tb["country"] = "Greenland"
    tb_members = pr.concat([tb_members, greenland_tb], ignore_index=True)

    # Improve table format.
    tb_members = tb_members.format(["country", "year"])
    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb_members], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
