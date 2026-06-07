"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def adherence_to_joined_year(tb):
    """Add a column with the year of joining the IWC.
    The adherence day is given as dd/mm/yy, so we extract the year from it. Years >48 are in the 1900s, while years <=48 are in the 2000s."""
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
            tb.at[_, "year_joined"] = year

    return tb


def add_leaving_year(tb):
    """Add a column with the year of leaving the IWC.
    The following countries have left the IWC:
    Panama - ~1980
    Canada - 1982
    Jamaica - ~1986
    Egypt - ~1986–92
    Philippines - ~1986–92
    Mauritius - ~1986–92
    Venezuela - ~1986–92
    Seychelles - 1994
    Japan - 2019
    """
    leaving_years = {
        "Panama": 1980,
        "Canada": 1982,
        "Jamaica": 1986,
        "Egypt": 1992,
        "Philippines": 1992,
        "Mauritius": 1992,
        "Venezuela": 1992,
        "Seychelles": 1994,
        "Japan": 2019,
    }
    tb["year_left"] = None
    for _, row in tb.iterrows():
        country = row["country"]
        if country in leaving_years:
            tb.at[_, "year_left"] = leaving_years[country]

    return tb


def create_member_tb(tb):
    """Create a table with one row per country and year, indicating whether the country was a member of the IWC in that year."""
    tb_rows = []
    for year in range(1946, 2026):
        for _, row in tb.iterrows():
            country = row["country"]
            year_joined = row["year_joined"]
            year_left = row["year_left"]
            member = False
            former_member = False
            if year_joined is not None and year_joined <= year:
                if year_left is not None and year >= year_left:
                    former_member = True
                else:
                    member = True
            tb_rows.append(
                {
                    "country": country,
                    "year": year,
                    "member": member,
                    "former_member": former_member,
                    "year_joined": year_joined if member is True else None,
                    "year_left": year_left if former_member is True else None,
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

    # Add year of joining column.
    tb = adherence_to_joined_year(tb)

    # drop adherence column
    tb = tb.drop(columns=["adherence"])

    # Add year of leaving column.
    tb = add_leaving_year(tb)

    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb=tb)

    tb_metadata = tb.metadata
    tb_origins = tb["country"].m.origins

    # Create member table.
    tb_members = create_member_tb(tb)
    tb_members.metadata = tb_metadata
    for col in tb_members.columns:
        tb_members[col].m.origins = tb_origins

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
