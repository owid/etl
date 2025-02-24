"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset, grapher_checks

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("unaids")

    #
    # Process data.
    #
    tables = [
        ds_garden["epi"],
        ds_garden["gam"],
        ds_garden["gam_sex"],
        ds_garden["gam_age"],
        ds_garden["gam_group"],
        ds_garden["gam_estimates"],
        ds_garden["gam_hepatitis"],
        ds_garden["gam_age_group"],
        ds_garden["gam_sex_group"],
        ds_garden["gam_age_sex"],
        ds_garden["gam_age_sex_group"],
    ]

    ##################################################################################################
    #
    # DEBUG: The following lines provide insights on which indicators are most complete.
    #
    # import owid.catalog.processing as pr
    # from etl.config import OWID_ENV
    # import pandas as pd

    # # Get completeness by indicator-dimension
    # tbs = []
    # for tb in tables:
    #     print(tb.m.short_name)
    #     uri =  str(tb.m.short_name)
    #     index_cols = tb.index.names

    #     tbx = tb.reset_index()
    #     tbx = tbx.melt(index_cols, var_name="indicator", value_name="value")
    #     tbx = tbx.dropna(subset=["value"])

    #     group_cols = ["indicator"] + [col for col in index_cols if col not in ["country", "year"]]
    #     tbx = tbx.groupby(group_cols).agg({"country": "nunique", "value": "count"}).reset_index()
    #     tbx["indicator"] = str(uri) + "#" + tbx["indicator"]

    #     tbs.append(tbx)
    # tbx = pr.concat(tbs)
    # cols_last = ["value", "country"]
    # tbx = tbx[[col for col in tbx.columns if col not in cols_last] + cols_last]
    # tbx = tbx.sort_values(["value", "country"], ascending=False)
    # tbx["uri_complete"] = (ds_garden.m.uri + "/" + tbx["indicator"]).str.replace("garden", "grapher")

    # # Get indicators in use
    # query = """
    # WITH t1 AS (
    #     SELECT
    #         *
    #     FROM variables
    #     WHERE catalogPath IS NOT NULL AND variables.datasetId = 6905
    # ),
    # t AS (
    # SELECT
    #     id,
    #     CASE
    #         WHEN catalogPath LIKE '%%__%%' THEN SUBSTRING_INDEX(catalogPath, '__', 1)
    #         ELSE catalogPath
    #     END AS uri_complete
    # FROM t1
    # )
    # SELECT
    #     t.uri_complete,
    #     count(DISTINCT cd.chartId) num_charts,
    #     GROUP_CONCAT(DISTINCT cd.chartId ORDER BY cd.chartId ASC) AS chart_ids
    # FROM chart_dimensions cd
    #     JOIN t
    #     ON t.id = cd.variableId
    #     GROUP BY uri_complete
    #     ORDER BY num_charts desc;
    # """
    # df = OWID_ENV.read_sql(query)
    # tbx = pd.DataFrame(tbx).merge(df, on="uri_complete", how="left")
    # tbx["num_charts"] = tbx["num_charts"].fillna(0).astype(int)

    # # Summary by indicator (aggregate over dimensions)
    # tbg = tbx.groupby("indicator", as_index=False).agg({
    #     "value": ["mean", "size"],
    #     "country": "mean",
    #     "num_charts": "max",
    #     "chart_ids": "unique",
    # })
    # tbg.columns = ["indicator", "num_values", "num_dimensions", "num_countries", "num_charts", "charts"]
    # tbg = tbg.sort_values(["num_values", "num_countries"], ascending=False)
    ##################################################################################################

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir,
        tables=tables,
        default_metadata=ds_garden.metadata,
    )

    #
    # Checks.
    #
    grapher_checks(ds_grapher)

    # Save changes in the new grapher dataset.
    ds_grapher.save()
