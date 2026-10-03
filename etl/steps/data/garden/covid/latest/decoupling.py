"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table

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
    tb_usa = ds_meadow["decoupling_usa"].reset_index()
    tb_israel = ds_meadow["decoupling_israel"].reset_index()

    #
    # Process data.
    #
    tb_spain = process_spain(tb_spain)
    tb_israel = process_israel(tb_israel)
    # tb_usa = process_usa(tb_israel)

    # Combine
    tb = combine_tables(tb_spain, tb_israel, tb_usa)

    # Format
    tb = tb.format(["country", "date"], short_name="decoupling")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def process_spain(tb: Table) -> Table:
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

    tb = tb.rename(
        columns={
            "Country": "country",
        }
    )

    return tb


def process_israel(tb: Table) -> Table:
    tb = (
        tb.loc[
            :,
            [
                "date",
                "new_infected",
                "new_serious",
                "new_deaths",
                "easy",
                "medium",
                "hard",
            ],
        ]
        .rename(
            columns={
                "new_infected": "confirmed_cases",
                "new_serious": "icu_flow",
                "new_deaths": "confirmed_deaths",
            }
        )
        .sort_values("date")
        .head(-1)
    )

    tb["hospital_stock"] = tb["easy"] + tb["medium"] + tb["hard"]

    vars = ["confirmed_cases", "icu_flow", "confirmed_deaths"]
    tb[vars] = tb[vars].rolling(7).sum()

    tb["date"] = pd.to_datetime(tb["date"])

    tb = adjust_x_and_y(
        tb=tb,
        start_date="2020-11-15",
        end_date="2021-04-01",
        hosp_variable="hospital_stock",
        icu_variable="icu_flow",
    )

    tb["country"] = "Israel"

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


def combine_tables(tb_spain: Table, tb_israel: Table, tb_usa: Table) -> Table:
    """Combine all tables."""
    tb = pr.concat([tb_usa, tb_spain, tb_israel], ignore_index=True)
    tb = tb[
        [
            "country",
            "date",
            "confirmed_cases",
            "hospital_flow",
            "hospital_stock",
            "icu_flow",
            "icu_stock",
            "confirmed_deaths",
        ]
    ]
    return tb
