from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Convert megawatts to gigawatts.
MW_TO_GW = 1e-3


def run() -> None:
    #
    # Load inputs.
    #
    # Load dataset from Garden and read its main table.
    ds_garden = paths.load_dataset("renewable_capacity_statistics")
    tb = ds_garden.read("renewable_capacity_statistics", reset_index=False)

    # Add all indicators also in gigawatts.
    for column in tb.columns:
        new_column = column + "_gw"
        tb[new_column] = tb[column] * MW_TO_GW
        # Update metadata fields.
        tb[new_column].metadata.title = tb[column].metadata.title + " (GW)"
        tb[new_column].metadata.unit = "gigawatts"
        tb[new_column].metadata.short_unit = "GW"
        tb[new_column].metadata.description_short = (
            tb[column].metadata.description_short.replace("mega", "giga").replace("MW", "GW")
        )

    #
    # Save outputs.
    #
    # Create new dataset.
    ds_grapher = paths.create_dataset(tables=[tb])
    ds_grapher.save()
