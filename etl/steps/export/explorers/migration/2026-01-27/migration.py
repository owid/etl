"""Load a grapher dataset and create an explorer dataset with its tsv file."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load dataset without including data
    ds_unicef = paths.load_dataset("child_migration")
    ds_refugee = paths.load_dataset("refugee_data")
    ds_migrant_stock = paths.load_dataset("migrant_stock")
    ds_wpp = paths.load_dataset("un_wpp")
    ds_wdi = paths.load_dataset("wdi")
    ds_idmc = paths.load_dataset("internal_displacement")

    # Read table from grapher dataset without loading data
    tb_unicef = ds_unicef.read("child_migration", load_data=False)
    tb_refugee_asylum = ds_refugee.read("refugee_data_asylum", load_data=False)
    tb_refugee_origin = ds_refugee.read("refugee_data_origin", load_data=False)
    tb_migrant_stock = ds_migrant_stock.read("migrant_stock", load_data=False)
    tb_wpp = ds_wpp.read("migration", load_data=False)
    tb_wdi = ds_wdi.read("wdi", load_data=False)
    tb_idmc = ds_idmc.read("internal_displacement", load_data=False)

    # Load grapher config from YAML
    config = paths.load_collection_config()

    # Create collection
    c = paths.create_collection(
        config=config,
        short_name="migration",
        explorer=True,
    )

    # Edit display
    tables = {
        "child_migration": tb_unicef,
        "refugee_data_asylum": tb_refugee_asylum,
        "refugee_data_origin": tb_refugee_origin,
        "migrant_stock": tb_migrant_stock,
        "migration": tb_wpp,
        "wdi": tb_wdi,
        "internal_displacement": tb_idmc,
    }

    for view in c.views:
        assert view.indicators.y is not None
        if len(view.indicators.y) == 1:
            tb_path = view.indicators.y[0].catalogPath.split("/")[-1]
            tb_name = tb_path.split("#")[0]
            col_name = tb_path.split("#")[1]
            tb = tables[tb_name]
            if col_name == "net_migration__sex_all__age_all__variant_estimates":
                view.config["title"] = "Net migration"  # type: ignore[assignment]
            elif col_name == "net_migration_rate__sex_all__age_all__variant_estimates":
                view.config["title"] = "Net migration rate"  # type: ignore[assignment]
            else:
                view.config["title"] = tb[col_name].metadata.title  # ty:ignore[invalid-assignment]
            view.config["subtitle"] = tb[col_name].metadata.description_short  # ty:ignore[invalid-assignment]
            var_pres = tb[col_name].metadata.presentation
            note = var_pres.grapher_config.get("note") if var_pres is not None else None
            if note:
                view.config["note"] = note  # type: ignore[assignment]

    c.save()
