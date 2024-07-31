"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("decoupling")

    # Read table from meadow dataset.
    tb_spain = ds_meadow["decoupling_spain"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(
        df=tb_spain,
        countries_file=paths.country_mapping_path,
    )
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def process_esp(tb: Table) -> Table:
    """Process spain table."""
    tb = (
        tb[["fecha", "num_casos", "num_hosp", "num_uci", "num_def"]]
        .rename(
            columns={
                "fecha": "date",
                "num_def": "confirmed_deaths",
                "num_casos": "confirmed_cases",
                "num_hosp": "hospital_flow",
                "num_uci": "icu_flow",
            }
        )
        .groupby("date", as_index=False)
        .sum()
        .assign(Country="Spain")
        .sort_values("date")
        .head(-8)
    )

    tb[["confirmed_cases", "confirmed_deaths", "hospital_flow", "icu_flow"]] = (
        tb[["confirmed_cases", "confirmed_deaths", "hospital_flow", "icu_flow"]].rolling(7).sum()
    )

    tb["date"] = pd.to_datetime(tb["date"])

    tb = adjust_x_and_y(
        tb,
        start_date="2020-12-15",
        end_date="2021-03-01",
        hosp_variable="hospital_flow",
        icu_variable="icu_flow",
    )

    return tb


def adjust_x_and_y(
    tb: Table,
    start_date: str,
    end_date: str,
    hosp_variable: str,
    icu_variable: str,
) -> Table:
    tb = tb.loc[tb["date"] >= start_date].copy()

    tb_period = tb[(tb["date"] >= start_date) & (tb["date"] <= end_date)].copy()
    case_peak_date = tb_period.sort_values("confirmed_cases")["date"].values[-1]
    hosp_peak_date = tb_period.sort_values(hosp_variable)["date"].values[-1]
    icu_peak_date = tb_period.sort_values(icu_variable)["date"].values[-1]
    death_peak_date = tb_period.sort_values("confirmed_deaths")["date"].values[-1]

    case_peak = tb.loc[tb["date"] == case_peak_date, "confirmed_cases"].values[0]
    hosp_peak = tb.loc[tb["date"] == hosp_peak_date, hosp_variable].values[0]
    icu_peak = tb.loc[tb["date"] == icu_peak_date, icu_variable].values[0]
    death_peak = tb.loc[tb["date"] == death_peak_date, "confirmed_deaths"].values[0]

    hosp_shift = (pd.to_datetime(hosp_peak_date) - pd.to_datetime(case_peak_date)).days
    icu_shift = (pd.to_datetime(icu_peak_date) - pd.to_datetime(case_peak_date)).days
    death_shift = (pd.to_datetime(death_peak_date) - pd.to_datetime(case_peak_date)).days

    tb[hosp_variable] = tb[hosp_variable].shift(-hosp_shift)
    tb[icu_variable] = tb[icu_variable].shift(-icu_shift)
    tb["confirmed_deaths"] = tb.confirmed_deaths.shift(-death_shift)

    tb["confirmed_cases"] = (100 * tb.confirmed_cases / case_peak).round(1)
    tb[hosp_variable] = (100 * tb[hosp_variable] / hosp_peak).round(1)
    tb[icu_variable] = (100 * tb[icu_variable] / icu_peak).round(1)
    tb["confirmed_deaths"] = (100 * tb.confirmed_deaths / death_peak).round(1)

    return tb
