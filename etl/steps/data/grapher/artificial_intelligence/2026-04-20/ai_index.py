"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    ds_garden = paths.load_dataset("ai_index_report")

    tb_adoption = ds_garden.read("ai_adoption", reset_index=False)
    tb_jobs = ds_garden.read("ai_jobs", reset_index=False)
    tb_conferences = ds_garden.read("ai_conferences", reset_index=False)
    tb_bills = ds_garden.read("ai_bills", reset_index=False)
    tb_incidents = ds_garden.read("ai_incidents", reset_index=False)
    tb_private_investment = ds_garden.read("ai_private_investment", reset_index=False)
    tb_corporate = ds_garden.read("ai_corporate_investment", reset_index=False)
    tb_total_private_investment = ds_garden.read("ai_total_investment_private", reset_index=False)
    tb_generative = ds_garden.read("ai_investment_generative", reset_index=False)
    tb_companies = ds_garden.read("ai_new_companies", reset_index=False)
    tb_robots_industrial = ds_garden.read("industrial_robots", reset_index=False)
    tb_robots_professional = ds_garden.read("professional_robots", reset_index=False)

    # Rename dimension indexes to "country" for grapher compatibility
    tb_conferences = tb_conferences.rename_index_names({"conference": "country"})
    tb_robots_professional = tb_robots_professional.rename_index_names({"application_area": "country"})
    tb_corporate = tb_corporate.rename_index_names({"investment_type": "country"})

    ds_grapher = paths.create_dataset(
        tables=[
            tb_adoption,
            tb_jobs,
            tb_conferences,
            tb_bills,
            tb_incidents,
            tb_robots_industrial,
            tb_robots_professional,
            tb_private_investment,
            tb_corporate,
            tb_generative,
            tb_companies,
            tb_total_private_investment,
        ],
        check_variables_metadata=True,
    )
    ds_grapher.save()
