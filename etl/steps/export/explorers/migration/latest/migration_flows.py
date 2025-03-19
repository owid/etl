from etl.collections.explorer import combine_config_dimensions, expand_config

# from etl.db import get_engine
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    # engine = get_engine()
    # Load configuration from adjacent yaml file.
    config = paths.load_mdim_config()

    # Add views for all dimensions
    ds = paths.load_dataset("migration_stock_flows")
    tb = ds.read("migrant_stock_dest_origin", load_data=False)

    # # sort columns by country select
    # dim_col = sorted([c for c in tb.columns if "migrants_all_sexes" in c])
    # tb = tb[["country", "year"] + dim_col]

    # # add country names and slugs to the config
    # cty_idx = [i for i, d in enumerate(config["dimensions"]) if d["slug"] == "country_select"][0]

    # all_countries = sorted(tb["country"].unique())
    # cty_dict_ls = [{"slug": c.lower(), "name": c} for c in all_countries]
    # config["dimensions"][cty_idx]["choices"] = cty_dict_ls

    # Define common view configuration
    common_view_config = {
        "type": "LineChart",
        "hasMapTab": True,
        "tab": "map",
        # "map": {
        #     "tooltipUseCustomLabels": True,
        #     "colorScale": {
        #         "binningStrategy": "manual",
        #         "baseColorScheme": "YlGnBu",
        #         "customNumericColorsActive": True,
        #         "customNumericMinValue": -1,
        #         "customNumericValues": [-1, 1000, 10000, 100000, 1000000, 0],
        #         "customNumericColors": ["#AF1629", None, None, None, None, None, None, None],
        #         "customNumericLabels": ["Selected Country", None, None, None, None, None, None, None],
        #     },
        # },
        "note": 'For most countries, immigrant means "born in another country". Someone who has gained citizenship in the country they live in is still counted as an immigrant if they were born elsewhere. For some countries, place of birth information is not available; in this case citizenship is used to define whether someone counts as an immigrant.',
    }

    # 2: Bake config automatically from table
    config_new = expand_config(
        tb,
        indicator_names=["migrants_all_sexes"],
        dimensions=["metric", "country_select"],
        indicators_slug="migrants",
        common_view_config=common_view_config,
    )

    # 3: Combine both sources (basically dimensions and views)
    config["dimensions"] = combine_config_dimensions(
        config_dimensions=config_new["dimensions"],
        config_dimensions_yaml=config.get("dimensions", {}),
    )
    config["views"] = config_new["views"]

    # 4: Upsert to DB
    explorer = paths.create_explorer(
        config=config,
        explorer_name="migration-flows",
    )
    explorer.save()
