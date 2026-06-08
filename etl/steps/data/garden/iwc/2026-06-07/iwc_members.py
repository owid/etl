"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

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
    {"country": "Panama", "withdrawal_year": 1980, "rejoining_year": None},
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

    For countries who have withdrawn and rejoined the year should be the original joining year. These are:

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
            if year > 48:
                year += 1900
            else:
                year += 2000
            if row["country"] in og_joining_years:
                year = og_joining_years[row["country"]]
            tb.at[_, "year_joined"] = year

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
    for year in range(1946, 2026):
        for _, row in tb.iterrows():
            country = row["country"]
            year_joined = row["year_joined"]
            year_left = row["year_left"]
            gaps = row["gaps"] if "gaps" in row.index else []
            member = False
            former_member = False
            effective_year_joined = year_joined
            effective_year_left = year_left

            if year_joined is not None and year_joined <= year:
                # Walk through gaps chronologically to find effective join/leave years.
                # If a rejoining year is passed, it becomes the new effective_year_joined
                # and clears any permanent leaving year that predates it.
                in_gap = False
                for withdrawal_year, rejoining_year in sorted(gaps):
                    if year < withdrawal_year:
                        break
                    elif withdrawal_year <= year < rejoining_year:
                        in_gap = True
                        effective_year_left = withdrawal_year
                        break
                    else:  # year >= rejoining_year
                        effective_year_joined = rejoining_year
                        # Country rejoined after a gap — reset any earlier permanent leaving year
                        if effective_year_left is not None and effective_year_left <= rejoining_year:
                            effective_year_left = None

                if in_gap:
                    former_member = True
                elif effective_year_left is not None and year >= effective_year_left:
                    former_member = True
                else:
                    member = True

            tb_rows.append(
                {
                    "country": country,
                    "year": year,
                    "member": member,
                    "former_member": former_member,
                    "year_joined": effective_year_joined if member else None,
                    "year_left": effective_year_left if former_member else None,
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

    # Add year_left (will be filled in add_gaps_and_left_periods)
    tb["year_left"] = None

    tb_metadata = tb.metadata
    tb_origins = tb["country"].m.origins

    # Create member table.
    tb_members = create_member_tb(tb)
    tb_members.metadata = tb_metadata
    for col in tb_members.columns:
        tb_members[col].m.origins = tb_origins

    # Add gaps and leaving periods.
    tb_members = add_gaps_and_left_periods(tb_members)

    # add member/ former member column
    tb_members["current_status"] = tb_members.apply(
        lambda row: "Member" if row["member"] else ("Former Member" if row["former_member"] else "Non-Member"), axis=1
    )

    # make years integers
    tb_members["year"] = tb_members["year"].astype(int)
    tb_members["year_joined"] = tb_members["year_joined"].astype("Int64")
    tb_members["year_left"] = tb_members["year_left"].astype("Int64")

    # Improve table format.
    tb_members = tb_members.format(["country", "year"])
    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb_members], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
