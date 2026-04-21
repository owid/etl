"""Single garden step for AI Index Report 2026 - consolidates all AI index datasets."""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def read_table(ds, name: str) -> Table:
    """Read meadow table and drop any leftover 'index' artifact column."""
    return ds.read(name).reset_index().drop(columns=["index"], errors="ignore")


def adjust_for_cpi(tb: Table, value_col: str, cpi_table: Table) -> Table:
    """Convert a column from nominal billions USD to constant 2021 USD."""
    tb = pr.merge(tb, cpi_table[["year", "cpi_adj_2021"]], on="year", how="inner")
    tb[value_col] = (tb[value_col] * 1e9 / tb["cpi_adj_2021"]).round()
    return tb.drop(columns=["cpi_adj_2021"])


def run() -> None:
    ds_meadow = paths.load_dataset("ai_index_report")
    ds_old_adoption = paths.load_dataset("ai_adoption")
    ds_us_cpi = paths.load_dataset("us_consumer_prices")

    tb_us_cpi = ds_us_cpi.read("us_consumer_prices").reset_index()
    cpi_2021 = tb_us_cpi.loc[tb_us_cpi["year"] == 2021, "all_items"].values[0]
    tb_us_cpi["cpi_adj_2021"] = tb_us_cpi["all_items"] / cpi_2021

    tables = []

    # ── AI Adoption ─────────────────────────────────────────────────────────────
    tb_adoption = read_table(ds_meadow, "ai_adoption")
    # New data: pct_of_respondents is decimal (0-1) → convert to percentage
    tb_adoption["pct_of_respondents"] = tb_adoption["pct_of_respondents"] * 100
    # New data uses longer region labels; normalize to shorter names for time-series continuity
    region_rename = {
        "Developing markets (incl. India, Central/South America, MENA)": "Developing markets",
        "Greater China (incl. Hong Kong, Taiwan, Macau)": "Greater China",
    }
    tb_adoption["country"] = tb_adoption["country"].replace(region_rename)

    # Append historical adoption data from old garden (years not in new data)
    tb_adoption_old = ds_old_adoption.read("ai_adoption").reset_index().drop(columns=["index"], errors="ignore")
    new_years = set(tb_adoption["year"].unique())
    tb_adoption_old = tb_adoption_old[~tb_adoption_old["year"].isin(new_years)]
    tb_adoption = pr.concat([tb_adoption_old, tb_adoption], ignore_index=True)
    tb_adoption = tb_adoption.format(["country", "year"])
    tables.append(tb_adoption)

    # ── AI Bills ────────────────────────────────────────────────────────────────
    tb_bills = read_table(ds_meadow, "ai_bills")
    tb_bills = paths.regions.harmonize_names(tb_bills, warn_on_unused_countries=False)
    tb_bills = tb_bills.format(["country", "year"])
    tables.append(tb_bills)

    # ── AI Incidents ─────────────────────────────────────────────────────────────
    tb_incidents = read_table(ds_meadow, "ai_incidents")
    tb_incidents = tb_incidents.format(["year", "country"])
    tables.append(tb_incidents)

    # ── AI Conferences ───────────────────────────────────────────────────────────
    tb_conferences = read_table(ds_meadow, "ai_conferences")
    # Multiply by 1000 and round to remove floating-point artifacts (raw data is in thousands)
    tb_conferences["attendees"] = (tb_conferences["number_of_attendees__in_thousands"] * 1000).round().astype("Int64")
    tb_conferences = tb_conferences.drop(columns=["number_of_attendees__in_thousands"])
    tb_conferences = tb_conferences.format(["conference", "year"])
    tables.append(tb_conferences)

    # ── AI Jobs ──────────────────────────────────────────────────────────────────
    tb_jobs = read_table(ds_meadow, "ai_jobs")
    # New data: % is decimal (0-1) → convert to percentage
    tb_jobs["ai_job_postings__pct_of_all_job_postings"] = tb_jobs["ai_job_postings__pct_of_all_job_postings"] * 100
    tb_jobs = tb_jobs.rename(columns={"ai_job_postings__pct_of_all_job_postings": "ai_job_postings_share"})
    tb_jobs = tb_jobs.format(["country", "year"])
    tables.append(tb_jobs)

    # ── Investment: Generative AI ────────────────────────────────────────────────
    tb_gen = read_table(ds_meadow, "ai_investment_generative")
    tb_gen = tb_gen.rename(columns={"total_investment__in_billions_of_us_dollars": "generative_ai"})
    tb_gen = adjust_for_cpi(tb_gen, "generative_ai", tb_us_cpi)
    tb_gen = tb_gen.format(["year", "country"])
    tb_gen.metadata.short_name = "ai_investment_generative"
    tables.append(tb_gen)

    # ── Investment: Total private (World) ────────────────────────────────────────
    tb_world = read_table(ds_meadow, "ai_investment_world")
    tb_world = tb_world.rename(columns={"total_investment__in_billions_of_us_dollars": "private_investment"})
    tb_world = adjust_for_cpi(tb_world, "private_investment", tb_us_cpi)
    tb_world = tb_world.format(["year", "country"])
    tb_world.metadata.short_name = "ai_total_investment_private"
    tables.append(tb_world)

    # ── Investment: Newly funded companies ──────────────────────────────────────
    tb_companies = read_table(ds_meadow, "ai_investment_companies")
    tb_companies = tb_companies.rename(columns={"number_of_companies": "companies"})
    tb_companies = tb_companies.format(["year", "country"])
    tb_companies.metadata.short_name = "ai_new_companies"
    tables.append(tb_companies)

    # ── Investment: Private by region ────────────────────────────────────────────
    tb_region = read_table(ds_meadow, "ai_investment_by_region")
    tb_region = tb_region.rename(columns={"total_investment__in_billions_of_us_dollars": "private_investment"})
    tb_region = adjust_for_cpi(tb_region, "private_investment", tb_us_cpi)
    tb_region = tb_region.format(["year", "country"])
    tb_region.metadata.short_name = "ai_private_investment"
    tables.append(tb_region)

    # ── Investment: Corporate deals by event type ────────────────────────────────
    tb_corp = read_table(ds_meadow, "ai_investment_corporate")
    tb_corp = tb_corp.rename(columns={"funding_in_usd__billions": "corporate_investment"})
    # Add total row per year
    tb_total_per_year = tb_corp.groupby("year", as_index=False)["corporate_investment"].sum()
    tb_total_per_year["investment_type"] = "Total"
    tb_corp = pr.concat([tb_corp, tb_total_per_year], ignore_index=True)
    tb_corp["investment_type"] = tb_corp["investment_type"].replace(
        {
            "Merger/Acquisition": "Merger/acquisition",
            "Minority Stake": "Minority stake",
            "Private Investment": "Private investment",
            "Public Offering": "Public offering",
        }
    )
    tb_corp = adjust_for_cpi(tb_corp, "corporate_investment", tb_us_cpi)
    # investment_type becomes the country dimension (like conferences/professional robots)
    tb_corp = tb_corp.format(["year", "investment_type"])
    tb_corp.metadata.short_name = "ai_corporate_investment"
    tables.append(tb_corp)

    # ── Industrial Robots ─────────────────────────────────────────────────────────
    tb_ind = read_table(ds_meadow, "ai_robots_industrial")
    tb_ind["number_of_robots"] = (tb_ind["number_of_robots__in_thousands"] * 1000).round().astype("Int64")
    tb_ind = tb_ind.drop(columns=["number_of_robots__in_thousands"])
    tb_ind = paths.regions.harmonize_names(tb_ind, warn_on_unused_countries=False)
    tb_ind = tb_ind.pivot(index=["year", "country"], columns="indicator", values="number_of_robots").reset_index()
    tb_ind.columns.name = None
    tb_ind = tb_ind.rename(
        columns={
            "Installations": "industrial_robot_installations",
            "Operational stock": "industrial_robot_stock",
        }
    )
    tb_ind = tb_ind.format(["country", "year"])
    tb_ind.metadata.short_name = "industrial_robots"
    tables.append(tb_ind)

    # ── Professional Robots ───────────────────────────────────────────────────────
    tb_prof = read_table(ds_meadow, "ai_robots_professional")
    # Raw CSV is in thousands of robots; convert to actual units
    tb_prof["number_of_professional_robots_installed"] = (tb_prof["amount"] * 1000).round().astype("Int64")
    tb_prof = tb_prof.drop(columns=["amount"])
    tb_prof["application_area"] = tb_prof["application_area"].replace(
        {
            "Medical and Healthcare": "Medical and healthcare",
            "Professional Cleaning": "Professional cleaning",
        }
    )
    tb_prof = tb_prof.format(["year", "application_area"])
    tb_prof.metadata.short_name = "professional_robots"
    tables.append(tb_prof)

    ds_garden = paths.create_dataset(
        tables=tables,
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )
    ds_garden.save()
