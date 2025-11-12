"""Load a garden dataset and create a grapher dataset."""

import pandas as pd
import structlog

from etl.helpers import PathFinder

log = structlog.get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("gho")

    #
    # Process data.
    #
    tables = []
    for tb_name in ds_garden.table_names:
        tb = ds_garden[tb_name]

        # They say it's in millions, but it's actually in thousands.
        col = "stunting_numbers_among_children_under_5_years_of_age__millions__model_based_estimates"
        if tb_name == col:
            tb[[col, col + "_low", col + "_high"]] /= 1000

        # There are zero values for countries which are obviously wrong.
        if tb_name == "proportion_of_population_with_primary_reliance_on_clean_fuels_and_technologies_for_cooking__pct":
            x = tb["proportion_of_population_with_primary_reliance_on_clean_fuels_and_technologies_for_cooking__pct"]
            for country in ("Bulgaria", "Libya", "Lebanon"):
                assert (
                    x.xs(country, level="country") == 0
                ).all(), "These countries have zero values by mistake. If they get fixed, remove this hotfix."
                tb.loc[tb.index.get_level_values("country") == country, :] = pd.NA

        # Invalid data from GHO, drop them for now.
        if tb_name == "attribution_of_road_traffic_deaths_to_alcohol__pct":
            col = "attribution_of_road_traffic_deaths_to_alcohol__pct"
            tb[col] = pd.to_numeric(tb[col], errors="coerce").copy_metadata(tb[col])

        # Drop noisy dimensions dhs_mics_subnational_regions__health_equity_monitor
        if "dhs_mics_subnational_regions__health_equity_monitor" in tb.index.names:
            tb = tb.query("dhs_mics_subnational_regions__health_equity_monitor.isnull()")
            tb = tb.reset_index(["dhs_mics_subnational_regions__health_equity_monitor"], drop=True)

        if (
            tb_name
            == "measles_containing_vaccine_second_dose__mcv2__immunization_coverage_by_the_nationally_recommended_age__pct"
        ):
            # Fix typo in description from producer.
            col = "measles_containing_vaccine_second_dose__mcv2__immunization_coverage_by_the_nationally_recommended_age__pct"
            error = "Expected typo in description from producer. It may have been fixed, so, remove this patch."
            assert "patters" in tb[col].metadata.description_from_producer, error
            tb[col].metadata.description_from_producer = tb[col].metadata.description_from_producer.replace(
                "patters", "patterns"
            )

        if tb_name in [
            "deaths_due_to_tuberculosis_among_hiv_negative_people__per_100_000_population",
            "number_of_deaths_due_to_tuberculosis__excluding_hiv",
        ]:
            # The same typo is found in all columns.
            error = "Expected typo in description from producer. It may have been fixed, so, remove this patch."
            for column in tb.columns:
                assert "Millenium" in tb[column].metadata.description_from_producer, error
                tb[column].metadata.description_from_producer = tb[column].metadata.description_from_producer.replace(
                    "Millenium", "Millennium"
                )

        if tb.empty:
            log.warning(f"Table '{tb_name}' is empty. Skipping.")
            continue

        tb = tb.drop(columns=["comments"], errors="ignore")

        tables.append(tb)

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = paths.create_dataset(tables=tables, check_variables_metadata=True, default_metadata=ds_garden.metadata)

    # Save changes in the new grapher dataset.
    ds_grapher.save()
