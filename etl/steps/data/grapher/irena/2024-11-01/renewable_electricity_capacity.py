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
    ds_garden = paths.load_dataset("renewable_electricity_capacity")
    tb = ds_garden["renewable_electricity_capacity"]

    # Add all indicators also in gigawatts.
    for column in tb.columns:
        new_column = column + "_gw"
        tb[new_column] = tb[column] * MW_TO_GW
        # Update metadata fields.
        for field in ["title", "unit", "short_unit", "description_short"]:
            setattr(
                tb[new_column].metadata,
                field,
                getattr(tb[column].metadata, field).replace("mega", "giga").replace("MW", "GW"),
            )

    #
    # Save outputs.
    #
    # Create new dataset.
    ds_grapher = create_dataset(dest_dir=dest_dir, tables=[tb], check_variables_metadata=True)
    ds_grapher.save()
