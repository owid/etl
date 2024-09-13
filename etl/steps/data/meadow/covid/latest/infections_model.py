"""Load a snapshot and create a meadow dataset."""

from owid.datautils.dataframes import multi_merge

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


column_rename = {
    "Entity": "country",
    "Day": "date",
    "Daily new estimated infections of COVID-19 (ICL, mean)": "icl_infections_mean",
    "Daily new estimated infections of COVID-19 (ICL, lower)": "icl_infections_low",
    "Daily new estimated infections of COVID-19 (ICL, upper)": "icl_infections_upper",
    "Daily new estimated infections of COVID-19 (IHME, mean)": "ihme_infections_mean",
    "Daily new estimated infections of COVID-19 (IHME, lower)": "ihme_infections_low",
    "Daily new estimated infections of COVID-19 (IHME, upper)": "ihme_infections_upper",
    "Daily new estimated infections of COVID-19 (LSHTM, mean)": "lshtm_infections_mean",
    "Daily new estimated infections of COVID-19 (LSHTM, lower)": "lshtm_infections_low",
    "Daily new estimated infections of COVID-19 (LSHTM, upper)": "lshtm_infections_upper",
    "Daily new estimated infections of COVID-19 (LSHTM, median)": "lshtm_infections_median",
    "Daily new estimated infections of COVID-19 (YYG, mean)": "yyg_infections_mean",
    "Daily new estimated infections of COVID-19 (YYG, lower)": "yyg_infections_low",
    "Daily new estimated infections of COVID-19 (YYG, upper)": "yyg_infections_upper",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshots.
    tables = []
    names = [
        "icl",
        "ihme",
        "lshtm",
        "youyang",
    ]
    for name in names:
        # Read
        tb_ = paths.read_snap_table(f"infections_model_{name}.csv")
        # Rename columns, keep relevant
        tb_ = tb_.rename(columns=column_rename)
        tb_ = tb_.drop(
            columns=[
                "Code",
                "Daily new confirmed cases due to COVID-19 (rolling 7-day average, right-aligned)",
            ]
        )

        # Format
        # tb_ = tb_.format(["country", "date"], short_name=name)
        # Add to list
        tables.append(tb_)

    # Merge
    tb = multi_merge(tables, on=["country", "date"], how="outer")
    # Drop all-NaN rows
    tb = tb.dropna(subset=[col for col in tb.columns if col not in {"country", "date"}], how="all")

    tb = tb.format(["country", "date"], short_name="infections_model")
    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
