"""Load the inequality-trends garden dataset and push its summary tables to grapher.

The per-country metrics panel goes to the database as-is; the reference-year aggregation goes as
the entity "World" (it covers the common PIP-and-WID sample), with the reference year as the time
dimension. The per-country change table stays garden-only.
"""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    ds_garden = paths.load_dataset("inequality_trends_by_reference_year")
    tb_metrics = ds_garden.read("inequality_metrics", reset_index=False)
    tb_trends = ds_garden.read("inequality_change_by_reference_year")

    #
    # Process data.
    #
    # The aggregation is global (over the common country sample); grapher needs an entity.
    tb_trends["country"] = "World"
    tb_trends = tb_trends.format(
        ["country", "year", "series", "metric"], short_name="inequality_change_by_reference_year"
    )

    #
    # Save outputs.
    #
    ds_grapher = paths.create_dataset(tables=[tb_metrics, tb_trends], default_metadata=ds_garden.metadata)
    ds_grapher.save()
