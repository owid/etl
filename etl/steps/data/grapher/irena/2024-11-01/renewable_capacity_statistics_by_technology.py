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

    #
    # Process data.
    #
    # Get the human-readable names of the technologies from the variable metadata.
    rename_technologies = {variable: tb[variable].metadata.display["name"] for variable in tb.columns}

    # Simplify table to consider only the World.
    # Here we use "country" to refer to a technology.
    # This is a workaround, so that grapher will let us select technologies as it does with countries.
    tb = tb.loc["World"].reset_index().melt(id_vars="year", var_name="country", value_name="capacity")

    # Rename technologies conveniently.
    tb = tb.replace(rename_technologies)

    # Set appropriate metadata.
    tb["capacity"].metadata.title = "Capacity"
    tb["capacity"].metadata.display = {"numDecimalPlaces": 0}

    # Convert units from megawatts to gigawatts.
    tb["capacity"] *= MW_TO_GW
    # Update metadata fields.
    for field in ["title", "unit", "short_unit", "description_short"]:
        setattr(
            tb["capacity"].metadata,
            field,
            getattr(tb["capacity"].metadata, field).replace("mega", "giga").replace("MW", "GW"),
        )

    # Improve table format.
    tb = tb.format(short_name="renewable_capacity_statistics_by_technology")

    # Update table's metadata.
    tb.metadata.title = "Renewable electricity capacity by technology"

    #
    # Save outputs.
    #
    # Create new dataset.
    ds_grapher = create_dataset(dest_dir=dest_dir, tables=[tb], check_variables_metadata=True)
    ds_grapher.save()
