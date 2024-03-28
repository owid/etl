"""
Construct the Chartbook of Economic Inequality dataset.

It comprises tables from multiple datasets, constructed as a long format table.
"""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden datasets.
    ds_altimir = paths.load_dataset("altimir_1986")
    ds_sedlac = paths.load_dataset("sedlac")

    # Load garden tables
    tb_sedlac = ds_sedlac["sedlac"].reset_index()
    tb_altimir = ds_altimir["altimir_1986"].reset_index()

    #
    # Process data.
    # SEDLAC

    # Select only Argentina and Brazil
    tb_sedlac = tb_sedlac[tb_sedlac["country"].isin(["Argentina", "Brazil"])].reset_index(drop=True)

    # Keep only gini and national_headcount_ratio
    tb_sedlac = tb_sedlac[["country", "year", "survey_number", "survey", "table", "national_headcount_ratio", "gini"]]

    # Rename table names
    tb_sedlac["table"] = tb_sedlac["table"].replace(
        {"50% of median": "50_median", "Equivalized income": "equivalized_income"}
    )

    # Filter tables
    tb_sedlac = tb_sedlac[tb_sedlac["table"].isin(["50_median", "equivalized_income"])].reset_index(drop=True)

    tb_sedlac.to_csv("tb_sedlac.csv")

    # Make the table wider, by using table as a column
    tb_sedlac = (
        tb_sedlac.pivot(
            index=["country", "survey_number", "survey", "year"],
            columns="table",
            values=["national_headcount_ratio", "gini"],
        )
        .sort_index()
        .reset_index()
    )

    # Flatten the multi-index columns
    tb_sedlac.columns = ["_".join(col).strip() for col in tb_sedlac.columns.values]

    # Remove "_" at the end of the column names
    tb_sedlac.columns = [col[:-1] if col.endswith("_") else col for col in tb_sedlac.columns]

    # Remove empty columns
    tb_sedlac = tb_sedlac.dropna(axis=1, how="all")

    # Rename columns
    tb_sedlac = tb_sedlac.rename(
        columns={
            "national_headcount_ratio_50_median": "headcount_ratio_50_median",
            "gini_equivalized_income": "gini",
        }
    )

    tb_sedlac['short_reference'] = tb_sedlac.m.origins.

    # ARGENTINA
    # Select Argentina in SEDLAC
    tb_sedlac_arg = tb_sedlac[tb_sedlac["country"] == "Argentina"].reset_index(drop=True)

    tb = tb.set_index(["country", "year", "spell", "spell_name"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
