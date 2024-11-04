from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Convert megawatts to gigawatts.
MW_TO_GW = 1e-3


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load dataset from Garden and read its main table.
    ds_garden = paths.load_dataset("renewable_capacity_statistics")
    tb = ds_garden["renewable_capacity_statistics"]

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
    ds_grapher = create_dataset(dest_dir=dest_dir, tables=[tb], check_variables_metadata=True)
    ds_grapher.save()
