"""Load a snapshot and create the World Values Survey - Trust meadow dataset."""

import pandas as pd
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def rename_vars(df: pd.DataFrame) -> pd.DataFrame:
    """
    This function renames the variables created with a loop, from trust and confidence questions
    """

    # This is a dictionary with the meaning of the codes
    vars_dict = {
        "D001_B": "Family",
        "E069_01": "Churches",
        "E069_02": "Armed Forces",
        "E069_03": "Education System",
        "E069_04": "Press",
        "E069_05": "Labour Unions",
        "E069_06": "Police",
        "E069_07": "Parliament",
        "E069_08": "Civil Services",
        "E069_09": "Social Security System",
        "E069_10": "Television",
        "E069_11": "Government",
        "E069_12": "Political Parties",
        "E069_13": "Major Companies",
        "E069_14": "Environmental Protection Movement",
        "E069_15": "Women's Movement",
        "E069_17": "Justice System/Courts",
        "E069_18": "European Union",
        "E069_18A": "Major regional organization (combined from country-specific)",
        "E069_19": "NATO",
        "E069_20": "United Nations",
        "E069_21": "Arab League",
        "E069_22": "Association of South East Asian Nations -ASEAN",
        "E069_23": "Organization for African Unity-OAU",
        "E069_24": "NAFTA",
        "E069_25": "Andean pact",
        "E069_26": "Mercosur",
        "E069_27": "SAARC",
        "E069_28": "ECO",
        "E069_29": "APEC",
        "E069_30": "Free Commerce Treaty (Tratado de libre comercio)",
        "E069_31": "United American States Organization",
        "E069_32": "“Movimiento en pro de Vieques”(Puerto Rico)",
        "E069_33": "Local/Regional Government",
        "E069_34": "SADC/SADEC",
        "E069_35": "East African Cooperation (EAC)",
        "E069_38": "Presidency",
        "E069_39": "Civil Society Groups",
        "E069_40": "Charitable or humanitarian organizations",
        "E069_41": "Banks",
        "E069_42": "CARICOM",
        "E069_43": "CIS",
        "E069_44": "Confidence in CER with Australia",
        "E069_45": "International Monetary Found (IMF)",
        "E069_46": "Non governmental Organizations (NGOs)",
        "E069_47": "American Forces",
        "E069_48": "Non-Iraqi television",
        "E069_49": "TV News",
        "E069_51": "Religious leaders",
        "E069_52": "Evangelic Church",
        "E069_54": "Universities",
        "E069_55": "Organization of the Islamic World",
        "E069_56": "Organization of American States (OAE)",
        "E069_57": "UNASUR",
        "E069_58": "Arab Maghreb Union",
        "E069_59": "Cooperation Council for the Arab states of Gulf (GCC)",
        "E069_60": "Mainland government",
        "E069_61": "World Trade Organization (WTO)",
        "E069_62": "World Health Organization (WHO)",
        "E069_63": "World Bank",
        "E069_64": "Elections",
        "E069_65": "International Criminal Court (ICC)",
        "E069_66": "UNDP United Nations Development Programme",
        "E069_67": "African Union (AU)",
        "G007_18_B": "Neighborhood",
        "G007_35_B": "Another religion",
        "G007_36_B": "Another nationality",
    }

    # Rename columns, replacing var with name when the original name ends with var
    for var, name in vars_dict.items():
        df = df.rename(columns={column: column.replace(var, name) for column in df.columns if column.endswith(var)})

    # Generate snake case names
    df.columns = df.columns.str.lower().str.replace(" ", "_")

    # Rename columns from confidence to trust
    df = df.rename(
        columns={
            "confidence_family": "trust_family",
            "confidence_neighborhood": "trust_neighborhood",
            "confidence_another_religion": "trust_another_religion",
            "confidence_another_nationality": "trust_another_nationality",
        },
        errors="raise",
    )

    return df


def run(dest_dir: str) -> None:
    log.info("wvs_trust.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap: Snapshot = paths.load_dependency("wvs_trust.csv")

    # Load data from snapshot.
    df = pd.read_csv(snap.path)

    #
    # Process data.
    df = rename_vars(df)

    # Verify index and sort
    df = df.set_index(["country", "year"], verify_integrity=True).sort_index()

    # Create a new table and ensure all columns are snake-case.
    tb = Table(df, short_name=paths.short_name, underscore=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("wvs_trust.end")
