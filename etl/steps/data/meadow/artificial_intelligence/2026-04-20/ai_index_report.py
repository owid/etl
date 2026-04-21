"""Load AI Index Report 2026 data from zip snapshot and create meadow tables."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder

paths = PathFinder(__file__)

ZIP_PREFIX = "PUBLIC DATA_ 2026 AI INDEX REPORT/"
CHAPTER_FOLDERS = {
    1: "1. Research and Development",
    3: "3. Responsible AI",
    4: "4. Economy",
    8: "8. Policy and Governance",
}


def fig_path(chapter: int, fig: str) -> str:
    return f"{ZIP_PREFIX}{CHAPTER_FOLDERS[chapter]}/Data/{fig}.csv"


def run() -> None:
    snap = paths.load_snapshot("ai_index_report.zip")

    with snap.extracted() as archive:
        # AI adoption (fig_4.3.2): % of Respondents (decimal), Geographic Area, Label (=survey year)
        tb_adoption = archive.read(fig_path(4, "fig_4.3.2"))
        tb_adoption.metadata.short_name = "ai_adoption"
        tb_adoption = tb_adoption.rename(columns={"Label": "year", "Geographic Area": "country"})
        tb_adoption = tb_adoption.format(["country", "year"])

        # AI bills (fig_8.4.2): Country, Number of AI-related bills passed into law
        tb_bills = archive.read(fig_path(8, "fig_8.4.2"))
        tb_bills.metadata.short_name = "ai_bills"
        tb_bills = tb_bills.rename(columns={"Country": "country"})
        tb_bills["year"] = 2024
        # Sum bills across duplicate country entries (e.g. different parliamentary chambers)
        tb_bills = tb_bills.groupby(["country", "year"], as_index=False).sum()
        tb_bills = tb_bills.format(["country", "year"])

        # AI incidents (fig_3.2.1): Year, Number of AI incidents
        tb_incidents = archive.read(fig_path(3, "fig_3.2.1"))
        tb_incidents.metadata.short_name = "ai_incidents"
        tb_incidents["country"] = "World"
        tb_incidents = tb_incidents.rename(columns={"Year": "year"})
        tb_incidents = tb_incidents.format(["year", "country"])

        # Conferences: combine fig_1.6.3 (total) + fig_1.6.4 (by conference)
        tb_conf_total = archive.read(fig_path(1, "fig_1.6.3"))
        tb_conf_by = archive.read(fig_path(1, "fig_1.6.4"))
        tb_conf_total["Label"] = "Total"
        tb_conferences = pr.concat([tb_conf_total, tb_conf_by], ignore_index=True)
        tb_conferences.metadata.short_name = "ai_conferences"
        tb_conferences = tb_conferences.rename(columns={"Year": "year", "Label": "conference"})
        tb_conferences = tb_conferences.format(["year", "conference"])

        # Investment: corporate deals by event type (fig_4.2.1)
        tb_investment_corporate = archive.read(fig_path(4, "fig_4.2.1"))
        tb_investment_corporate.metadata.short_name = "ai_investment_corporate"
        tb_investment_corporate = tb_investment_corporate.rename(
            columns={"Year": "year", "Event Type": "investment_type"}
        )
        tb_investment_corporate = tb_investment_corporate.format(["year", "investment_type"])

        # Investment: total private World (fig_4.2.2)
        tb_investment_world = archive.read(fig_path(4, "fig_4.2.2"))
        tb_investment_world.metadata.short_name = "ai_investment_world"
        tb_investment_world = tb_investment_world.rename(columns={"Year": "year"})
        tb_investment_world["country"] = "World"
        tb_investment_world = tb_investment_world.format(["year", "country"])

        # Investment: generative AI (fig_4.2.3)
        tb_investment_generative = archive.read(fig_path(4, "fig_4.2.3"))
        tb_investment_generative.metadata.short_name = "ai_investment_generative"
        tb_investment_generative = tb_investment_generative.rename(columns={"Year": "year"})
        tb_investment_generative["country"] = "World"
        tb_investment_generative = tb_investment_generative.format(["year", "country"])

        # Investment: newly funded companies — World (fig_4.2.4) + by region (fig_4.2.14)
        tb_companies_world = archive.read(fig_path(4, "fig_4.2.4"))
        tb_companies_world = tb_companies_world.rename(columns={"Year": "year"})
        tb_companies_world["country"] = "World"
        tb_companies_by_region = archive.read(fig_path(4, "fig_4.2.14"))
        tb_companies_by_region = tb_companies_by_region.rename(columns={"Year": "year", "Label": "country"})
        tb_investment_companies = pr.concat([tb_companies_world, tb_companies_by_region], ignore_index=True)
        tb_investment_companies.metadata.short_name = "ai_investment_companies"
        tb_investment_companies = tb_investment_companies.format(["year", "country"])

        # Investment: by region (fig_4.2.11): Year, Total investment, Label (=region)
        tb_investment_by_region = archive.read(fig_path(4, "fig_4.2.11"))
        tb_investment_by_region.metadata.short_name = "ai_investment_by_region"
        tb_investment_by_region = tb_investment_by_region.rename(columns={"Year": "year", "Label": "country"})
        tb_investment_by_region = tb_investment_by_region.format(["year", "country"])

        # Jobs: combine fig_4.4.1 + fig_4.4.2 (Year, AI job postings %, Label=country)
        tb_jobs1 = archive.read(fig_path(4, "fig_4.4.1"))
        tb_jobs2 = archive.read(fig_path(4, "fig_4.4.2"))
        tb_jobs = pr.concat([tb_jobs1, tb_jobs2], ignore_index=True)
        tb_jobs.metadata.short_name = "ai_jobs"
        tb_jobs = tb_jobs.rename(columns={"Year": "year", "Label": "country"})
        tb_jobs = tb_jobs.format(["year", "country"])

        # Industrial robots: combine installations (4.5.1), stock (4.5.2), by country (4.5.5)
        tb_robots_installed = archive.read(fig_path(4, "fig_4.5.1"))
        tb_robots_installed["Country"] = "World"
        tb_robots_installed["Indicator"] = "Installations"
        tb_robots_installed = tb_robots_installed.rename(
            columns={"Number of industrial robots installed (in thousands)": "Number of robots (in thousands)"}
        )

        tb_robots_stock = archive.read(fig_path(4, "fig_4.5.2"))
        tb_robots_stock["Country"] = "World"
        tb_robots_stock["Indicator"] = "Operational stock"
        tb_robots_stock = tb_robots_stock.rename(
            columns={"Number of industrial robots (in thousands)": "Number of robots (in thousands)"}
        )

        tb_robots_by_country = archive.read(fig_path(4, "fig_4.5.5"))
        tb_robots_by_country["Indicator"] = "Installations"
        tb_robots_by_country = tb_robots_by_country.rename(
            columns={"Number of industrial robots installed (in thousands)": "Number of robots (in thousands)"}
        )

        tb_robots_industrial = pr.concat(
            [tb_robots_installed, tb_robots_stock, tb_robots_by_country], ignore_index=True
        )
        tb_robots_industrial.metadata.short_name = "ai_robots_industrial"
        tb_robots_industrial = tb_robots_industrial.rename(columns={"Year": "year", "Country": "country"})
        tb_robots_industrial = tb_robots_industrial.format(["year", "country", "indicator"])

        # Professional robots (fig_4.5.7): year, application, amount
        tb_robots_professional = archive.read(fig_path(4, "fig_4.5.7"))
        tb_robots_professional.metadata.short_name = "ai_robots_professional"
        tb_robots_professional = tb_robots_professional.rename(columns={"application": "application_area"})
        tb_robots_professional = tb_robots_professional.format(["year", "application_area"])

    ds_meadow = paths.create_dataset(
        tables=[
            tb_adoption,
            tb_bills,
            tb_incidents,
            tb_conferences,
            tb_investment_corporate,
            tb_investment_world,
            tb_investment_generative,
            tb_investment_companies,
            tb_investment_by_region,
            tb_jobs,
            tb_robots_industrial,
            tb_robots_professional,
        ],
        check_variables_metadata=False,
        default_metadata=snap.metadata,
    )
    ds_meadow.save()
