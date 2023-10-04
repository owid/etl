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
    tb_cause = paths.load_dataset("gbd_cause")
    tb_hierarchy = paths.load_dataset("cause_hierarchy")
    tb_hierarchy = tb_hierarchy["cause_hierarchy"]

    # Select out the level 2 and level 3 causes separately, level 2 are broader than level 3
    tb_hierarchy["cause_name_underscore"] = tb_hierarchy["cause_name"].apply(underscore)

    level_2_causes = tb_hierarchy[tb_hierarchy["level"] == 2]["cause_name_underscore"].to_list()
    level_2_causes = list(map(lambda item: underscore(item), level_2_causes))

    level_3_causes = tb_hierarchy[tb_hierarchy["level"] == 3]["cause_name_underscore"].to_list()
    level_3_causes = list(map(lambda item: underscore(item), level_3_causes))

    tb_level_2 = create_hierarchy_table(tb_cause, level_2_causes, short_name="leading_cause_level_2")
    tb_level_3 = create_hierarchy_table(tb_cause, level_3_causes, short_name="leading_cause_level_3")

    tb_level_2 = clean_disease_names(tb=tb_level_2, tb_hierarchy=tb_hierarchy)
    tb_level_3 = clean_disease_names(tb=tb_level_3, tb_hierarchy=tb_hierarchy)

    tb_level_2 = tb_level_2.set_index(["country", "year"], verify_integrity=True)
    tb_level_3 = tb_level_3.set_index(["country", "year"], verify_integrity=True)
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb_level_2, tb_level_3], check_variables_metadata=True, default_metadata=tb_cause.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def create_hierarchy_table(tb_cause: Table, level_causes: List[str], short_name: str) -> Table:
    u5_causes = [item for item in tb_cause.table_names if "__under_5" in item]
    # u5_causes = list(map(lambda cause: cause.split("__")[0],  u5_causes))
    tb_out = Table()
    for cause in u5_causes:
        tb = tb_cause[cause].reset_index()
        cause_name = cause.split("__")[0]
        if cause_name in level_causes:
            death_col = f"deaths_that_are_from_{cause_name}__in_both_sexes_aged_under_5"
            if death_col in tb.columns:
                cols = ["country", "year", death_col]
                tb = tb[cols]
                tb = tb.rename(columns={death_col: cause_name})
                tb_out = pr.concat([tb_out, tb])

    long_tb = pr.melt(
        tb_out, id_vars=["country", "year"], var_name=f"disease_{short_name}", value_name=f"deaths_{short_name}"
    )

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
    tb = tb.merge(tb_hierarchy, how="left", left_on=disease_col, right_on="cause_name_underscore")
    tb = tb.drop(columns=["cause_name_underscore", disease_col])

    return tb
