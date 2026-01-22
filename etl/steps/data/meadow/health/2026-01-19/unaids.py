"""Load a snapshot and create a meadow dataset."""

import numpy as np

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

column_index = ["country", "year", "indicator", "dimension"]


VALUES_ROUNDED_EXPECTED_NAN_VALUES = {
    "epi": ["..."],
    "gam": [
        "Death penalty",
        "Laws penalizing same-sex sexual acts have been decriminalized or never existed",
        "Imprisonment or no penalty specified",
        "Any criminalization or punitive regulation of sex work",
        "Sex work is not subject to punitive regulations or is not criminalized",
        "Issue is determined/differs at subnational level",
        "Possession of drugs for personal use or drug use or consumption are not punished by laws or regulations",
        "Possession of drugs for personal use or drug use and/or consumption are specified as criminal offences",
        "Possession of drugs for personal use or drug use and/or consumption are specified as non-criminal offences",
        "Compulsory detention for drug offences",
        "4-High-income",
        "1-Low-income",
        "2-Lower-middle-income",
        "3-Upper-middle-income",
        "No",
        "Yes",
        "No but prosecutions exist based on general criminal laws",
        "Yes for adolescents younger than 18 years",
        "Yes for adolescents younger than 16 years",
        "Yes for adolescents younger than 12 years",
        "Deport prohibit short- and/or long-stay and require HIV testing or disclosure for some permits",
        "Require HIV testing or disclosure for some permits",
        "Prohibit short- and/or long-stay and require HIV testing or disclosure for some permits",
        "Asia and Pacific",
        "East and Southern Africa",
        "Eastern Europe and Central Asia",
        "Western and Central Europe and North America",
        "Middle East and North Africa",
        "Latin America",
        "Caribbean",
        "West and Central Africa",
        "Latin America and the Caribbean",
    ],
    "kpa": [
        "No",
        "Yes",
        "Yes imprisonment (up to 14 years)",
        "Laws penalizing same-sex sexual acts have been decriminalized or never existed",
        "Yes death penalty",
        "Yes imprisonment (14 years - life)",
        "No specific legislation",
        "Yes no penalty specified",
        "Any criminalization or punitive regulation of sex work",
        "Sex work is not subject to punitive regulations or is not criminalized",
        "No but prosecutions exist based on general criminal laws",
        "No restrictions",
        "Deport prohibit short- and/or long-stay and require HIV testing or disclosure for some permits",
        "Require HIV testing or disclosure for some permits",
        "Prohibit short- and/or long-stay and require HIV testing or disclosure for some permits",
    ],
}


def run() -> None:
    #
    # Load inputs.
    #
    short_names = [
        "epi",
        "gam",
        "kpa",
        # "ncpi",
    ]
    tables = []
    for name in short_names:
        paths.log.info(name)
        # Retrieve snapshot.
        short_name = f"unaids_{name}.zip"
        snap = paths.load_snapshot(short_name)

        # Load data from snapshot.
        tb = snap.read_csv()

        #
        # Process data.
        #
        tb = clean_table(tb, name)
        # Format table
        tb = tb.format(column_index, short_name=name)

        # Append current table to list of tables.
        tables.append(tb)

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables)

    # Save meadow dataset.
    ds_meadow.save()


def clean_table(tb, name):
    """Minor table cleaning."""
    paths.log.info(f"Formatting table {tb.m.short_name}")

    # Rename columns, only keep relevant
    columns = {
        "Indicator": "indicator",
        "Indicator_GId": "indicator_id",
        "Unit": "unit",
        "Subgroup": "dimension",
        "Subgroup_Val_GId": "dimension_id",
        "Area": "country",
        # "Area ID" : "",
        "Time Period": "year",
        "Source": "source",
        "Data value": "value",
        "Formatted": "value_rounded",
        "Data_Denominator": "data_denominator",
        "Footnote": "footnote",
    }
    tb = tb.rename(columns=columns)[columns.values()]

    # Sanity check: only one code per indicator
    assert tb.groupby("indicator")["indicator_id"].nunique().max() == 1
    assert tb.groupby("indicator_id")["indicator"].nunique().max() == 1

    # Drop duplicates
    tb = tb.drop_duplicates(subset=["country", "year", "indicator", "dimension"], keep="first")

    # Check NaN values: Check that only expected indicators have NaNs
    # We do this check because we are dropping all NaN values.
    # Note: Some non-NaN values are categorical indicators, whose values are captured in value_rounded.
    # Note 2: For some reason, there are some countries as indicators. Unclear why.
    assert (
        tb[tb["value"].isna()]["value_rounded"].dropna().unique() == VALUES_ROUNDED_EXPECTED_NAN_VALUES[name]
    ).all(), "Unexpected indicators with NaN values"
    # tb.loc[:, "value"] = tb["value"].replace("...", np.nan)
    tb = tb.dropna(subset=["value"])

    return tb
