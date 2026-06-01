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

        # Invalid data from GHO, drop them for now.
        if tb_name == "attribution_of_road_traffic_deaths_to_alcohol__pct":
            col = "attribution_of_road_traffic_deaths_to_alcohol__pct"
            tb[col] = pd.to_numeric(tb[col], errors="coerce").copy_metadata(tb[col])

        # Drop noisy dimensions dhs_mics_subnational_regions__health_equity_monitor
        if "dhs_mics_subnational_regions__health_equity_monitor" in tb.index.names:
            tb = tb.query("dhs_mics_subnational_regions__health_equity_monitor.isnull()")
            tb = tb.reset_index(["dhs_mics_subnational_regions__health_equity_monitor"], drop=True)

        ################################################################################################################
        # The same typos were found across many instances of description_from_producer.
        # Assert just one occurrence of each typo, and fix them everywhere.
        error = "Expected typo in description from producer. It may have been fixed, so, remove this patch."
        if tb_name == "deaths_due_to_tuberculosis_among_hiv_negative_people__per_100_000_population":
            assert (
                "Millenium"
                in tb[
                    "deaths_due_to_tuberculosis_among_hiv_negative_people__per_100_000_population"
                ].metadata.description_from_producer
            ), error
        if (
            tb_name
            == "measles_containing_vaccine_second_dose__mcv2__immunization_coverage_by_the_nationally_recommended_age__pct"
        ):
            assert (
                "patters"
                in tb[
                    "measles_containing_vaccine_second_dose__mcv2__immunization_coverage_by_the_nationally_recommended_age__pct"
                ].metadata.description_from_producer
            ), error
        for column in tb.columns:
            dfp = tb[column].metadata.description_from_producer
            if dfp and "Millenium" in dfp:
                tb[column].metadata.description_from_producer = dfp.replace("Millenium", "Millennium")
            if dfp and "patters" in dfp:
                tb[column].metadata.description_from_producer = dfp.replace("patters", "patterns")
        ################################################################################################################

        if tb.empty:
            log.warning(f"Table '{tb_name}' is empty. Skipping.")
            continue

        tb = tb.drop(columns=["comments"], errors="ignore")
        # Drop label/code columns that have no unit/title/origins — they are lookup metadata, not indicators.
        tb = tb.drop(columns=[c for c in tb.columns if c.startswith("ghe_cause_of_death_codes")], errors="ignore")

        tables.append(tb)

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = paths.create_dataset(tables=tables, check_variables_metadata=True, default_metadata=ds_garden.metadata)

    # Save changes in the new grapher dataset.
    ds_grapher.save()
