"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

CAUSES_MAP = {
    "#Diseases of heart (I00-I09,I11,I13,I20-I51)": "heart disease",
    "#Malignant neoplasms (C00-C97)": "cancer",
    "#Accidents (unintentional injuries) (V01-X59,Y85-Y86)": "accidents",
    "#Cerebrovascular diseases (I60-I69)": "stroke",
    "#Chronic lower respiratory diseases (J40-J47)": "respiratory",
    "#Alzheimer disease (G30)": "alzheimers",
    "#Diabetes mellitus (E10-E14)": "diabetes",
    "#Nephritis, nephrotic syndrome and nephrosis (N00-N07,N17-N19,N25-N27)": "kidney",
    "#Chronic liver disease and cirrhosis (K70,K73-K74)": "liver",
    "#COVID-19 (U07.1)": "covid",
    "#Intentional self-harm (suicide) (*U03,X60-X84,Y87.0)": "suicide",
    "#Influenza and pneumonia (J09-J18)": "influenza",
    "#Essential hypertension and hypertensive renal disease (I10,I12,I15)": "hypertension",
    "#Septicemia (A40-A41)": "septicemia",
    "#Parkinson disease (G20-G21)": "parkinson",
}


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("leading_causes.csv")

    # Load data from snapshot.
    tb = snap.read_csv(sep="\t", header=0)

    tb = tb.drop(columns=["Notes", "Population", "15 Leading Causes of Death Code"], errors="raise")
    tb = tb.rename(columns={"15 Leading Causes of Death": "full_icd_code"})
    tb["cause"] = tb["full_icd_code"].map(CAUSES_MAP)
    tb = tb.dropna(subset=["cause", "Deaths"], how="all")
    tb["year"] = 2023

    #
    # Process data.
    #
    # Improve tables format.
    tables = [tb.format(["cause", "year"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
