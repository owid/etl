"""Load a meadow dataset and create a garden dataset.

We only consider data from countries that are present in HMD. And, additionally, we only consider entries for these countries since the year they first appear in the HMD dataset (even if for that period we use UN WPP data, i.e. post-1950)
"""

from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Year to start tracking. Note that in the first years, few countries have data. Hence, we start in a later year, where more countries have data.
YEAR_FIRST = 1840


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("life_tables")
    ds_hmd = paths.load_dataset("hmd")

    # Read table from meadow dataset.
    tb = ds_meadow.read("life_tables", reset_index=False)
    tb_hmd = ds_hmd.read("life_tables")

    #
    # Process data.
    #
    # Filter relevant dimensions
    tb = tb.loc[(slice(None), slice(None), slice(None), "0", "period"), ["life_expectancy"]].reset_index()

    # Keep relevant columns and rows
    tb = tb.drop(columns=["type", "age"]).dropna()

    # Rename column
    tb = tb.rename(columns={"location": "country"})

    # Get country-sex and first year of LE reported in HMD
    tb_hmd = get_first_year_of_country_in_hmd(tb_hmd)

    # Only preserve countries coming from HDM
    tb = tb.merge(tb_hmd, on=["country", "sex"], suffixes=("", "_min"))
    tb = tb[tb["year"] >= tb["year_min"]].drop(columns=["year_min"])

    # Get max for each year
    tb = tb.loc[tb.groupby(["year", "sex"], observed=True)["life_expectancy"].idxmax()]

    # Organise columns
    tb["country_with_max_le"] = tb["country"]
    tb["country"] = tb["country"] + " " + tb["year"].astype("string")

    # First year
    tb = tb[tb["year"] >= YEAR_FIRST]

    # Set index
    tb = tb.format(["country", "year", "sex"], short_name="broken_limits_le")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def get_first_year_of_country_in_hmd(tb_hmd: Table) -> Table:
    tb_hmd = tb_hmd.loc[(tb_hmd["type"] == "period") & (tb_hmd["age"] == "0")]
    tb_hmd = tb_hmd.loc[:, ["country", "year", "sex", "life_expectancy"]].dropna()
    tb_hmd = tb_hmd.groupby(["country", "sex"], observed=True, as_index=False)["year"].min()
    return tb_hmd
