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

    # tbs = []
    # for tb in tables:
    #     print(tb.m.short_name)
    #     uri = tb.m.short_name
    #     index_cols = tb.index.names

    #     tbx = tb.reset_index()
    #     tbx = tbx.melt(index_cols, var_name="indicator", value_name="value")
    #     tbx = tbx.dropna(subset=["value"])

    #     group_cols = ["indicator"] + [col for col in index_cols if col not in ["country", "year"]]
    #     tbx = tbx.groupby(group_cols).agg({"country": "nunique", "value": "count"}).reset_index()
    #     tbx["indicator"] = str(uri) + "#" + tbx["indicator"]

    #     tbs.append(tbx)
    # import owid.catalog.processing as pr
    # tbx = pr.concat(tbs)
    # cols_last = ["value", "country"]
    # tbx = tbx[[col for col in tbx.columns if col not in cols_last] + cols_last]
    # tbx = tbx.sort_values(["value", "country"], ascending=False)

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
