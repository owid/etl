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
    #
    tables = [
        ds_garden["population"],
        ds_garden["growth_rate"],
        ds_garden["natural_change_rate"],
        ds_garden["fertility_rate"],
        ds_garden["migration"],
        ds_garden["deaths"],
        ds_garden["births"],
        ds_garden["median_age"],
        ds_garden["life_expectancy"],
        ds_garden["sex_ratio"],
        ds_garden["mortality_rate"],
        ds_garden["dependency_ratio"],
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
