"""Export the thousand-bins income distribution data for our bespoke income distribution chart.

This step uses the thousand_bins_distribution dataset dependency as its basis, and writes one
JSON file per year plus `metadata.json`, the feed's provenance derived from the garden columns
(see `etl.viz.bespoke`).

The files are written to the step's output folder; the framework syncs that folder to the R2 path
of the environment being built, so the feed is served at
`<root>/v1/bespoke/poverty_inequality/latest/income_distribution/income-distribution.<year>.json`
-- `api.ourworldindata.org` on production, and `api-staging.owid.io/<env>` on a staging server or
a laptop.

Run without --grapher to skip the upload and only write the local files.
"""

import json

import pandas as pd
from owid.catalog import Table
from tqdm.auto import tqdm

from etl.data_helpers.misc import round_to_sig_figs
from etl.helpers import PathFinder
from etl.viz.bespoke import build_feed_metadata, write_feed_metadata

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def prepare_distribution_table(tb: Table) -> pd.DataFrame:
    """Prepare one row per country-year for JSON export."""
    tb = tb.reset_index()
    # Sort by quantile before grouping so each country's avgs list runs from poorest to richest bin.
    tb = tb.sort_values(["year", "country", "region", "quantile"]).reset_index(drop=True)
    # Four significant figures keep the payload compact without meaningfully reducing precision.
    tb["avg"] = tb["avg"].apply(lambda value: round_to_sig_figs(value, sig_figs=4))

    tb_export = (
        tb.groupby(["year", "country", "region"], as_index=False, observed=True, sort=False)
        .agg(pop=("pop", "sum"), avgs=("avg", list))
        .sort_values(["year", "country"])
        .reset_index(drop=True)
    )
    tb_export["pop"] = tb_export["pop"].round().astype(int)
    tb_export["year"] = tb_export["year"].astype(int)

    invalid_bins = tb_export[tb_export["avgs"].str.len() != 1000]
    if not invalid_bins.empty:
        invalid_bins = invalid_bins.assign(n_bins=invalid_bins["avgs"].str.len())
        invalid_country_years = sorted(
            f"{row.year}: {row.country} ({row.n_bins} bins)"
            for row in invalid_bins[["year", "country", "n_bins"]].itertuples()
        )
        raise ValueError(f"Expected 1000 income bins for every country-year: {invalid_country_years}")

    duplicated = tb_export[tb_export.duplicated(["year", "country"], keep=False)]
    if not duplicated.empty:
        duplicated_countries = sorted(
            f"{row.year}: {row.country}" for row in duplicated[["year", "country"]].drop_duplicates().itertuples()
        )
        raise ValueError(f"Found duplicate country entries: {duplicated_countries}")

    return tb_export


def create_distribution_json(tb_export: pd.DataFrame, year: int) -> dict:
    """Create the income distribution JSON structure for a single year."""
    tb_year = tb_export.loc[tb_export["year"] == year]

    if tb_year.empty:
        raise ValueError(f"No thousand-bins distribution data found for year {year}.")

    return {
        "year": int(year),
        "data": {
            row["country"]: {
                "country": row["country"],
                "region": row["region"],
                "totalPopulation": row["pop"],
                "avgs": row["avgs"],
            }
            for row in tb_year.to_dict(orient="records")
        },
    }


def save_json(data: dict, filename: str) -> None:
    """Write one JSON file of the feed into the step's output folder."""
    paths.output_dir.mkdir(parents=True, exist_ok=True)
    with open(paths.output_dir / filename, "w") as f:
        json.dump(data, f, separators=(",", ":"))


def run() -> None:
    #
    # Load data.
    #
    paths.log.info("Loading thousand_bins_distribution dataset.")
    ds_garden = paths.load_dataset("thousand_bins_distribution")
    tb = ds_garden.read("thousand_bins_distribution", reset_index=False, safe_types=False)

    # The feed's provenance, derived from the origins of the data it is built on.
    write_feed_metadata(
        paths.output_dir,
        build_feed_metadata(
            title="Income distribution",
            columns={"Average income or consumption": tb["avg"], "Population": tb["pop"]},
            update_period_days=ds_garden.metadata.update_period_days,
        ),
    )

    #
    # Prepare data.
    #
    paths.log.info("Preparing income distribution table.")
    tb_export = prepare_distribution_table(tb)
    years = sorted(tb_export["year"].unique())

    #
    # Generate, save, and upload JSON files.
    #
    paths.log.info(f"Creating {len(years)} income distribution JSON files.")
    for year in tqdm(years, desc="Processing years"):
        data = create_distribution_json(tb_export, year=year)
        save_json(data, f"income-distribution.{year}.json")

    paths.log.info(f"Successfully created {len(years)} income distribution JSON files.")
