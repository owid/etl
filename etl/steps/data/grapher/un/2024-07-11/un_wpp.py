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
    ]
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


#################################################################################
#################################################################################
# Old code below. Left in case it's needed for reference.
#################################################################################
#################################################################################
def _filter_rows(table: Table) -> Table:
    """Keep only relevant fertility-scenarios.

    TODO: use or not?
    """
    variants_valid = ["estimates", "low", "medium", "high", "constant fertility"]
    shape_0 = table.shape[0]
    table = table[table.index.isin(variants_valid, level=4)]
    r = 100 - 100 * round(table.shape[0] / shape_0, 2)
    paths.log.info(f"Removed {r}% rows, by only keeping variants {variants_valid}")
    return table
