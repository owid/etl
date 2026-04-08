"""Load a snapshot and create a meadow dataset."""

from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("summary_reporting_system_estimates.csv")

    # Load data from snapshot.
    tb = snap.read()
    tb.loc[tb["state_abbr"].isna(), "state_abbr"] = "US"
    tb.loc[tb["state_abbr"] == "US", "state_name"] = "United States"
    # Some commas in numeric columns
    tb = tb.replace(",", "", regex=True)
    # Filter to US only
    tb_us = tb[tb["state_name"] == "United States"]
    tb_us = tb_us.rename(columns={"state_name": "country"})
    # Data for 2017-2020 is missing national values so we need to calculate them
    assert (~tb_us["year"].isin([2017, 2018, 2019, 2020])).all()

    tb_missing_years = tb[tb["year"].isin([2017, 2018, 2019, 2020])]
    cols = [
        "population",
        "violent_crime",
        "homicide",
        "rape_legacy",
        "rape_revised",
        "robbery",
        "aggravated_assault",
        "property_crime",
        "burglary",
        "larceny",
        "motor_vehicle_theft",
    ]

    tb_missing_years = tb_missing_years.copy()  # avoid SettingWithCopyWarning

    tb_missing_years.loc[:, cols] = tb_missing_years.loc[:, cols].apply(pr.to_numeric, errors="coerce")
    tb_missing_years = tb_missing_years.groupby("year").sum(numeric_only=True).reset_index()
    tb_missing_years["country"] = "United States"
    tb_missing_years["state_abbr"] = "US"
    tb = pr.concat([tb_us, tb_missing_years], ignore_index=True)

    # Process data.
    #
    # Improve tables format.
    tables = [tb.format(["country", "year"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
