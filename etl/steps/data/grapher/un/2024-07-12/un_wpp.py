from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("un_wpp")
    #
    # Process data.
    # Fiona: Removing the January data for now, we want it for Cologne but no need to change the live data for now.
    tb_pop = remove_jan_data(ds_garden["population"])
    tb_sex_ratio = remove_jan_data(ds_garden["sex_ratio"])
    tb_dependency = remove_jan_data(ds_garden["dependency_ratio"])
    # Import tables
    tables = [
        tb_pop,
        # ds_garden["population"],
        ds_garden["growth_rate"],
        ds_garden["natural_change_rate"],
        ds_garden["fertility_rate"],
        ds_garden["migration"],
        ds_garden["deaths"],
        ds_garden["births"],
        ds_garden["median_age"],
        ds_garden["life_expectancy"],
        tb_sex_ratio,
        # ds_garden["sex_ratio"],
        ds_garden["mortality_rate"],
        tb_dependency,
        # ds_garden["dependency_ratio"],
        ds_garden["mean_age_childbearing"],
    ]

    # Process distribution indicator
    # tb_dist = ds_garden["fertility_single"]
    # tb_dist = tb_dist.rename_index_names({"age": "year"})

    # Add to list of tables
    # tables = tables + [tb_dist]

    #
    # Save outputs.
    #
    # Create grapher dataset
    ds_grapher = create_dataset(
        dest_dir,
        tables=tables,
        check_variables_metadata=True,
        default_metadata=ds_garden.metadata,
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()


def remove_jan_data(tb: Table) -> Table:
    tb = tb.reset_index()
    tb = tb[tb["month"].isin(["july", "July"])].drop(columns=["month"]).copy()
    tb = tb.format(["country", "year", "sex", "age", "variant"])
    return tb
