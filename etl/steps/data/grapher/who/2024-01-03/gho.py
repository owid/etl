"""Load a garden dataset and create a grapher dataset."""

import structlog

from etl.helpers import PathFinder, create_dataset

log = structlog.get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
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

        # Drop noisy dimensions dhs_mics_subnational_regions__health_equity_monitor
        if "dhs_mics_subnational_regions__health_equity_monitor" in tb.index.names:
            tb = tb.query("dhs_mics_subnational_regions__health_equity_monitor.isnull()")
            tb = tb.reset_index(["dhs_mics_subnational_regions__health_equity_monitor"], drop=True)

        if tb.empty:
            log.warning(f"Table '{tb_name}' is empty. Skipping.")
            continue

        tb = tb.drop(columns=["comments"], errors="ignore")

        tables.append(tb)

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=tables, check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
