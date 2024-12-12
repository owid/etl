"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset and read its main table.
    ds_garden = paths.load_dataset("critical_minerals")
    tb_supply_by_country_flat = ds_garden.read("supply_by_country")

    #
    # Process data.
    #
    # Remove "World" from supply data, since charts will be stacked area
    # (and, by construction, all countries should always add up to World).
    tb_supply_by_country_flat = tb_supply_by_country_flat[tb_supply_by_country_flat["country"] != "World"].reset_index(
        drop=True
    )

    #
    # Improve format.
    #
    tb_supply_by_country_flat = tb_supply_by_country_flat.format()

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb_supply_by_country_flat], check_variables_metadata=True)
    ds_grapher.save()
