"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("clinical_trials")

    # Read tables from garden dataset.

    tb_trials_per_year = ds_garden.read("trials_per_year", reset_index=False)
    tb_sponsor_per_year = ds_garden.read("sponsor_per_year", reset_index=False)
    tb_study_type_per_year = ds_garden.read("study_type_per_year", reset_index=False)
    tb_purpose_per_year = ds_garden.read("purpose_per_year", reset_index=False)
    tb_status_per_year = ds_garden.read("status_per_year", reset_index=False)
    tb_length_per_year = ds_garden.read("length_per_year", reset_index=False)
    tb_intervention_per_year = ds_garden.read("interventions_per_year")
    tb_results_per_year = ds_garden.read("results_per_year", reset_index=False)
    tb_participants_per_year = ds_garden.read("participants_per_year", reset_index=False)

    tb_intervention_per_year["country"] = "World"
    tb_intervention_per_year = tb_intervention_per_year.format(["year", "country"])

    tables_ls = [
        tb_trials_per_year,
        tb_sponsor_per_year,
        tb_intervention_per_year,
        tb_study_type_per_year,
        tb_purpose_per_year,
        tb_status_per_year,
        tb_length_per_year,
        tb_results_per_year,
        tb_participants_per_year,
    ]
    #
    # Save outputs.
    #
    # Initialize a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=tables_ls, default_metadata=ds_garden.metadata)

    # Save grapher dataset.
    ds_grapher.save()
