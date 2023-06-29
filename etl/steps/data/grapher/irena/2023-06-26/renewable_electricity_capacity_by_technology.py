from owid import catalog

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # Load dataset from Garden and read its main table.
    ds_garden: catalog.Dataset = paths.load_dependency("renewable_electricity_capacity")
    tb = ds_garden["renewable_electricity_capacity"]

    # Get the human-readable names of the technologies from the variable metadata.
    rename_technologies = {variable: tb[variable].metadata.title for variable in tb.columns}

    # Simplify table to consider only the World.
    # Here we use "country" to refer to a technology.
    # This is a workaround, so that grapher will let us select technologies as it does with countries.
    tb = tb.loc["World"].reset_index().melt(id_vars="year", var_name="country", value_name="capacity")

    # Rename technologies conveniently.
    tb = tb.replace(rename_technologies)

    # Set appropriate metadata.
    tb["capacity"].metadata.title = "Capacity"
    tb["capacity"].metadata.display = {"numDecimalPlaces": 0}

    # Create new dataset.
    ds_grapher = create_dataset(dest_dir=dest_dir, tables=[tb], default_metadata=None)
    ds_grapher.metadata.title = f"Renewable electricity capacity by technology (IRENA, {paths.version.split('-')[0]})"
    ds_grapher.save()
