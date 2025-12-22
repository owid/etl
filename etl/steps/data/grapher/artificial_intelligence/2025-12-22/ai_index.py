"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden datasets.
    ds_garden_ai_adoption = paths.load_dataset("ai_adoption")
    ds_garden_ai_jobs = paths.load_dataset("ai_jobs")
    ds_garden_ai_conferences = paths.load_dataset("ai_conferences")
    ds_garden_bills = paths.load_dataset("ai_bills")
    ds_garden_ai_incidents = paths.load_dataset("ai_incidents")
    ds_garden_robots = paths.load_dataset("ai_robots")
    ds_garden_ai_investment = paths.load_dataset("ai_investment")

    # Read all of the tables from garden dataset.
    tb_adoption = ds_garden_ai_adoption.read("ai_adoption", reset_index=False)
    tb_jobs = ds_garden_ai_jobs.read("ai_jobs", reset_index=False)
    tb_conferences = ds_garden_ai_conferences.read("ai_conferences", reset_index=False)
    tb_bills = ds_garden_bills.read("ai_bills", reset_index=False)
    tb_incidents = ds_garden_ai_incidents.read("ai_incidents", reset_index=False)
    tb_private_investment = ds_garden_ai_investment.read("ai_private_investment", reset_index=False)
    tb_corporate = ds_garden_ai_investment.read("ai_corporate_investment", reset_index=False)
    tb_total_private_investment = ds_garden_ai_investment.read("ai_total_investment_private", reset_index=False)
    tb_generative = ds_garden_ai_investment.read("ai_investment_generative", reset_index=False)
    tb_companies = ds_garden_ai_investment.read("ai_new_companies", reset_index=False)
    tb_robots_industrial = ds_garden_robots.read("industrial_robots", reset_index=False)
    tb_robots_professional = ds_garden_robots.read("professional_robots", reset_index=False)
    tb_robots_professional = tb_robots_professional.rename_index_names({"application_area": "country"})

    # Rename for plotting model name as country in grapher
    tb_conferences = tb_conferences.rename_index_names({"conference": "country"})
    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
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

    # Save changes in the new grapher dataset.
    ds_grapher.save()
