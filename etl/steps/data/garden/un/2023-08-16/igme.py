"""Load a meadow dataset and create a garden dataset."""

from math import trunc

from owid.catalog import Table, VariableMeta

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("igme")

    # Read table from meadow dataset.
    tb = ds_meadow["igme"].reset_index()

    #
    # Process data.
    #
    tb = fix_sub_saharan_africa(tb)
    tb: Table = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb = filter_data(tb)
    tb = round_down_year(tb)
    tb = clean_values(tb)
    tb = tb.rename(
        columns={"obs_value": "Observation value", "lower_bound": "Lower bound", "upper_bound": "Upper bound"}
    )
    tb = tb.pivot(
        index=["country", "year"],
        values=["Observation value", "Lower bound", "Upper bound"],
        columns=["unit_of_measure", "indicator", "sex", "wealth_quintile"],
        join_column_levels_with="-",
    )
    # Add some metadata to the variables. Getting the unit from the column name and inferring the number of decimal places from the unit.
    # If it contains " per " we know it is a rate and should have 1 d.p., otherwise it should be an integer.
    for col in tb.columns[2:]:
        unit = col.split("-")[1]
        tb[col].metadata = VariableMeta(unit=unit.lower().strip())
        if " per " in unit:
            tb[col].metadata.display = {"numDecimalPlaces": 1}
        else:
            tb[col].metadata.display = {"numDecimalPlaces": 0}
    tb = tb.set_index(["country", "year"], verify_integrity=True)
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def filter_data(tb: Table) -> Table:
    """
    Filtering out the unnecessary columns and rows from the data.
    We just want the UN IGME estimates, rather than the individual results from the survey data.
    """
    # Keeping only the UN IGME estimates and the total wealth quintile
    tb = tb.loc[(tb["series_name"] == "UN IGME estimate")]

    cols_to_keep = [
        "country",
        "year",
        "indicator",
        "sex",
        "unit_of_measure",
        "wealth_quintile",
        "obs_value",
        "lower_bound",
        "upper_bound",
    ]
    # Keeping only the necessary columns.
    tb = tb[cols_to_keep]

    return tb


def clean_values(tb: Table) -> Table:
    """
    Adding clearer meanings to the values in the table.
    """
    sex_dict = {"Total": "Both sexes"}

    wealth_dict = {
        "Total": "All wealth quintiles",
        "Lowest": "Poorest quintile",
        "Highest": "Richest quintile",
        "Middle": "Middle wealth quintile",
        "Second": "Second poorest quintile",
        "Fourth": "Fourth poorest quintile",
    }

    tb["sex"] = tb["sex"].replace(sex_dict)

    tb["wealth_quintile"] = tb["wealth_quintile"].replace(wealth_dict)

    return tb


def fix_sub_saharan_africa(tb: Table) -> Table:
    """
    Sub-Saharan Africa appears twice in the Table, as it is defined by two different organisations, UNICEF and SDG.
    This function clarifies this by combining the region and organisation into one.
    """
    tb["country"] = tb["country"].astype(str)

    tb.loc[
        (tb["country"] == "Sub-Saharan Africa") & (tb["regional_group"] == "UNICEF"), "country"
    ] = "Sub-Saharan Africa (UNICEF)"

    tb.loc[
        (tb["country"] == "Sub-Saharan Africa") & (tb["regional_group"] == "SDG"), "country"
    ] = "Sub-Saharan Africa (SDG)"

    return tb


def round_down_year(tb: Table) -> Table:
    """
    Round down the year value given - to match what is shown on https://childmortality.org
    """

    tb["year"] = tb["year"].apply(trunc)

    return tb
