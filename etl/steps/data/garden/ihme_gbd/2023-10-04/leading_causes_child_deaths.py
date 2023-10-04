import owid.catalog.processing as pr
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder

# naming conventions
paths = PathFinder(__file__)
log = get_logger()


def run(dest_dir: str) -> None:
    tb_cause = paths.load_dataset("gbd_cause")
    u5_causes = [item for item in tb_cause.table_names if "__under_5" in item]
    tb_out = Table()
    for cause in u5_causes:
        tb = tb_cause[cause].reset_index()
        cause_name = cause.split("__")[0]
        death_col = f"deaths_that_are_from_{cause_name}__in_both_sexes_aged_under_5"
        if death_col in tb.columns:
            cols = ["country", "year", death_col]
            tb = tb[cols]
            tb = tb.rename(columns={death_col: cause_name})
            tb_out = pr.concat([tb_out, tb])

    long_tb = pr.melt(tb_out, id_vars=["country", "year"], var_name="disease", value_name="deaths")

    leading_causes_idx = long_tb.groupby(["country", "year"], observed=True)["deaths"].idxmax()
    leading_causes_tb = long_tb.loc[leading_causes_idx]
