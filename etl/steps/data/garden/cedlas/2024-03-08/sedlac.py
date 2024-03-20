"""Load a meadow dataset and create a garden dataset."""

from typing import Dict, List

import owid.catalog.processing as pr
from owid.catalog import Dataset, Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define the tables to be loaded from the meadow dataset.
POVERTY_TABLES = {
    "poverty_215": {"column_name": "$2.15 a day"},
    "poverty_365": {"column_name": "$3.65 a day"},
    "poverty_685": {"column_name": "$6.85 a day"},
    "poverty_median": {"column_name": "50% of median"},
}
INEQUALITY_INDICES_TABLES = {
    "ineq_indices_pci": {"column_name": "Per capita income"},
    "ineq_indices_ei": {"column_name": "Equivalized income"},
    # "ineq_indices_lmi": {"column_name": "lmi"},
    # "ineq_indices_ni": {"column_name": "ni"},
}

INEQUALITY_DECILES_TABLES = {
    "ineq_deciles_pci": {"column_name": "Per capita income"},
    "ineq_deciles_ei": {"column_name": "Equivalized income"},
    # "ineq_deciles_lmi": {"column_name": "lmi"},
    # "ineq_deciles_ni": {"column_name": "ni"},
}

INEQUALITY_GINI_TABLES = {
    "gini1": {"column_name": "gini"},
    "gini2": {"column_name": "gini"},
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("sedlac")

    # Read tables from meadow dataset.
    tb_poverty = load_tables(dataset=ds_meadow, tables_dict=POVERTY_TABLES)
    tb_ineq_indices = load_tables(dataset=ds_meadow, tables_dict=INEQUALITY_INDICES_TABLES)
    tb_ineq_deciles = load_tables(dataset=ds_meadow, tables_dict=INEQUALITY_DECILES_TABLES)

    #
    # Process data.
    tb_poverty = merge_tables(tables=tb_poverty, merge_or_concat="concat", short_name="poverty")
    tb_ineq_indices = merge_tables(tables=tb_ineq_indices, merge_or_concat="concat", short_name="ineq_indices")
    tb_ineq_deciles = merge_tables(tables=tb_ineq_deciles, merge_or_concat="concat", short_name="ineq_deciles")

    tb_inequality = merge_tables(
        tables=[tb_ineq_indices, tb_ineq_deciles], merge_or_concat="merge", short_name="inequality"
    )

    tb = merge_tables(tables=[tb_poverty, tb_inequality], merge_or_concat="merge", short_name="sedlac")

    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )

    # Remove urban and rural estimates from poverty table
    tb = drop_urban_rural_from_poverty_table(tb)

    # Remove duplicate rows and spells from tables, for Grapher datasets
    tb_no_spells = remove_spells_duplicates_and_add_urban(tb=tb, short_name="sedlac_no_spells")

    tb = tb.set_index(["country", "year", "survey_number", "survey", "table"], verify_integrity=True)
    tb_no_spells = tb_no_spells.set_index(["country", "year", "table"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[tb, tb_no_spells],
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def load_tables(dataset: Dataset, tables_dict: Dict) -> List[Table]:
    """Load tables from the meadow dataset."""

    tables = []
    for t in tables_dict:
        tb = dataset[t].reset_index()

        # Create a column with the name of the table
        tb["table"] = tables_dict[t]["column_name"]

        tables.append(tb)

    return tables


def merge_tables(tables: List[Table], merge_or_concat: str, short_name: str) -> Table:
    """Merge tables into a single table."""

    if merge_or_concat == "concat":
        # Concatenate tables
        tb = pr.concat(tables, ignore_index=True, short_name=short_name)
    elif merge_or_concat == "merge":
        # use the first table as base
        tb = tables[0]
        for i in range(len(tables) - 1):
            if i < len(tables) - 1:
                # Merge tables
                tb = pr.merge(
                    tb,
                    tables[i + 1],
                    on=["country", "year", "survey_number", "survey", "table"],
                    how="outer",
                    short_name=short_name,
                )

    else:
        raise ValueError("Invalid value for merge_or_concat: must be 'merge' or 'concat'.")

    return tb


def remove_spells_duplicates_and_add_urban(tb: Table, short_name: str) -> Table:
    """Remove duplicate rows and spells from a table."""

    tb = tb.copy()

    # Make years int by selecting the text of the first 4 characters
    tb["year"] = tb["year"].str[:4].astype(int)

    # Remove duplicates
    tb = tb.drop_duplicates(subset=["country", "year", "table"], keep="last", ignore_index=True)

    # Add "(urban)" to country names when needed
    # Make country string
    tb["country"] = tb["country"].astype(str)

    # Define the conditions to add "(urban)" to country names
    # In the case of Bolivia, I add the year condition to avoid adding "(urban)" to the whole poverty series (it only has one survey spell beginning in 2000)
    countries_conditions = [
        ("Argentina", tb["country"] == "Argentina"),
        ("Bolivia", (tb["country"] == "Bolivia") & (tb["survey_number"] == 1) & (tb["year"] <= 1997)),
        ("Uruguay", (tb["country"] == "Uruguay") & (tb["survey_number"] == 1)),
    ]

    for country, condition in countries_conditions:
        mask = condition
        tb.loc[mask, "country"] = tb.loc[mask, "country"] + " (urban)"

    # Drop survey columns
    tb = tb.drop(columns=["survey", "survey_number"])

    # Assign short_name to the table
    tb.m.short_name = short_name

    return tb


def drop_urban_rural_from_poverty_table(tb: Table) -> Table:
    """Drop urban and rural from poverty table"""
    tb = tb.copy()

    # Drop all columns containing urban or rural
    tb = tb[tb.columns[~tb.columns.str.contains("urban|rural", case=False)]]

    return tb
