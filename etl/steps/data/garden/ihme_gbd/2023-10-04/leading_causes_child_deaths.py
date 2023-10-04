from typing import List

import owid.catalog.processing as pr
from owid.catalog import Table
from owid.catalog.utils import underscore
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

# naming conventions
paths = PathFinder(__file__)
log = get_logger()


def run(dest_dir: str) -> None:
    # Load in the cause of death data and hierarchy of causes data
    tb_cause = paths.load_dataset("gbd_cause")
    tb_hierarchy = paths.load_dataset("cause_hierarchy")
    tb_hierarchy = tb_hierarchy["cause_hierarchy"]

    # Underschore the hierachy cause names to match tb_cause
    tb_hierarchy["cause_name_underscore"] = tb_hierarchy["cause_name"].apply(underscore)

    # We'll iterate through each level of the hierachy to find the leading cause of death in under-fives in each country-year
    levels = [1, 2, 3, 4]

    tb_out = []
    for level in levels:
        # Get the causes at this level
        level_causes = tb_hierarchy[tb_hierarchy["level"] == level]["cause_name_underscore"].to_list()
        # Create table with leading cause of death at this level for each country-year
        tb_level = create_hierarchy_table(
            age_group="__under_5",
            tb_cause=tb_cause,
            level_causes=level_causes,
            short_name=f"leading_cause_level_{level}",
        )
        # Make the disease names more readable
        tb_level = clean_disease_names(tb=tb_level, tb_hierarchy=tb_hierarchy)
        tb_level = tb_level.set_index(["country", "year"], verify_integrity=True)
        tb_out.append(tb_level)

    tb_level_1 = tb_out[0]
    tb_level_2 = tb_out[1]
    tb_level_3 = tb_out[2]
    tb_level_4 = tb_out[3]
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[tb_level_1, tb_level_2, tb_level_3, tb_level_4],
        check_variables_metadata=True,
        default_metadata=tb_cause.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def create_hierarchy_table(age_group: str, tb_cause: Table, level_causes: List[str], short_name: str) -> Table:
    u5_causes = [item for item in tb_cause.table_names if age_group in item]
    assert len(u5_causes) > 0, f"No causes found for {age_group}, check spelling"

    tb_out = Table()
    for cause in u5_causes:
        tb = tb_cause[cause].reset_index()
        # Get cause name from table name
        cause_name = cause.split("__")[0]
        if cause_name in level_causes:
            # Get deaths column from cause name
            death_col = f"deaths_that_are_from_{cause_name}__in_both_sexes_aged_under_5"
            if death_col in tb.columns:
                cols = ["country", "year", death_col]
                tb = tb[cols]
                tb = tb.rename(columns={death_col: cause_name})
                tb_out = pr.concat([tb_out, tb])

    # Melt the table from wide to long to make it easier to groupby
    long_tb = pr.melt(
        tb_out, id_vars=["country", "year"], var_name=f"disease_{short_name}", value_name=f"deaths_{short_name}"
    )
    # Find the cause of death with the highest number of deaths in each country-year
    leading_causes_idx = long_tb.groupby(["country", "year"], observed=True)[f"deaths_{short_name}"].idxmax()
    leading_causes_tb = long_tb.loc[leading_causes_idx]

    leading_causes_tb.metadata.short_name = short_name

    return leading_causes_tb


def clean_disease_names(tb: Table, tb_hierarchy: Table) -> Table:
    """
    Making the underscored disease names more readable using the original hierarchy table

    """

    tb_hierarchy = tb_hierarchy[["cause_name", "cause_name_underscore"]]
    disease_col = [item for item in tb.columns if "disease_" in item]
    disease_col = disease_col[0]
    tb = tb.merge(tb_hierarchy, how="left", left_on=disease_col, right_on="cause_name_underscore")
    tb = tb.drop(columns=["cause_name_underscore", disease_col])
    tb = tb.rename(columns={"cause_name": disease_col})

    return tb
