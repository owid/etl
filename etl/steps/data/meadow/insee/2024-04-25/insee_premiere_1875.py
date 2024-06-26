"""Load a snapshot and create a meadow dataset."""

from typing import Dict

from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define columns and their new names
COLUMNS_INEQUALITY = {
    "Unnamed: 0": "year",
    "Ratio \nInter-décile     D9/D1": "p90_p10_ratio",
    "Ratio\n(100-S80)/S20": "s80_s20_ratio",
    "Indice \nDe Gini ": "gini",
}

COLUMNS_POVERTY = {
    "Nombre de personnes pauvres (en milliers)": "headcount",
    "Taux de pauvreté (en %)": "headcount_ratio",
    "Seuil de pauvreté (en euros 2019/mois)": "poverty_line",
    "Niveau de vie médian des personnes pauvres (en euros 2019/mois)": "average_income_in_poverty",
    "Intensité de la pauvreté (en %)": "income_gap_ratio",
}

# Define relative poverty names
RELATIVE_POVERTY = {"Seuil à 60 % de la médiane": "60_median", "Seuil à 50 % de la médiane": "50_median"}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("insee_premiere_1875.xlsx")

    # Load data from snapshot.
    tb_inequality = snap.read(sheet_name="Figure 2", skiprows=2)
    tb_poverty = snap.read(sheet_name="Figure 3", skiprows=2)

    # Process data.
    tb_inequality = process_inequality_data(tb=tb_inequality, columns=COLUMNS_INEQUALITY, short_name="inequality")
    tb_poverty = process_poverty_data(
        tb=tb_poverty, columns=COLUMNS_POVERTY, relative_poverty_names=RELATIVE_POVERTY, short_name="poverty"
    )

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir, tables=[tb_inequality, tb_poverty], check_variables_metadata=True, default_metadata=snap.metadata
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def process_inequality_data(tb: Table, columns: Dict[str, str], short_name: str) -> Table:
    """
    Process the inequality data.
    """
    # Rename columns
    tb = tb.rename(columns=columns, errors="raise")

    # Make year integer
    # If year row is string, delete
    tb = tb[tb["year"].astype(str).str.isnumeric()].reset_index(drop=True)

    # If year row is float, convert to integer

    tb["year"] = tb["year"].astype(int)

    # Add country
    tb["country"] = "France"

    # Format
    tb = tb.format(short_name=short_name)

    return tb


def process_poverty_data(
    tb: Table, columns: Dict[str, str], relative_poverty_names: Dict[str, str], short_name: str
) -> Table:
    """
    Process the poverty data.
    """

    tb = tb.copy()

    # Rename first column
    tb = tb.rename(columns={"Unnamed: 0": "indicator"}, errors="raise")

    # Create a new column, relative_poverty, when the indicator is in relative_poverty_names.keys()
    tb["relative_poverty"] = tb["indicator"].map(relative_poverty_names)

    # Fill the NaN values in the relative_poverty column with the last non missing value
    tb["relative_poverty"] = tb["relative_poverty"].ffill()

    # Change the names of the indicator column using colums dictionary
    tb = tb[tb["indicator"].isin(columns.keys())].reset_index(drop=True)
    tb["indicator"] = tb["indicator"].map(columns)

    # Create indicator as the concatenation of relative_poverty and indicator
    tb["indicator"] = tb["indicator"] + "_" + tb["relative_poverty"]

    # Drop relative_poverty column
    tb = tb.drop(columns=["relative_poverty"])

    tb = tb.melt(id_vars=["indicator"], var_name="year", value_name="value")

    # Make table wide
    tb = tb.pivot(index="year", columns="indicator", values="value").reset_index()

    # Add country
    tb["country"] = "France"

    # Format
    tb = tb.format(short_name=short_name)

    return tb
