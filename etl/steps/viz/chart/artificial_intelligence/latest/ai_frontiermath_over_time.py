"""FrontierMath scores over time, with the models that set a new record selected by default."""

from owid.catalog import Table

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def frontier_models(tb: Table) -> list[str]:
    """Return the models that solved a larger share of problems than every model released before them.

    Models released on the same day are compared with each other too, so only the best of them can set a record.
    """
    tb = tb.sort_values(["date", "mean_score"], ascending=[True, False])
    best_before = tb["mean_score"].cummax().shift(fill_value=float("-inf"))
    return tb.loc[tb["mean_score"] > best_before, "country"].astype(str).tolist()


def run() -> None:
    # Load the scores the chart plots.
    tb = paths.load_dataset("frontiermath").read("epoch_benchmark_data")

    # Select the record-setting models, which change with every data update.
    config = paths.load_config()
    frontier = frontier_models(tb)
    assert frontier, "No model set a record: is the benchmark table empty?"
    config["views"][0]["config"]["selectedEntityNames"] = frontier

    c = paths.create_chart(config=config)
    c.save()
