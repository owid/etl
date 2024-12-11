from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Convert megawatts to gigawatts.
MW_TO_GW = 1e-3

# List of technologies to include, and how to rename them.
TECHNOLOGIES = {
    "Bioenergy": "Bioenergy (total)",
    "Biogas": "Biogas",
    # "Biogas (off-grid)": "Biogas (off-grid)",
    "Concentrated solar power": "Concentrated solar power",
    # "Geothermal": "Geothermal",
    # "Geothermal (off-grid)": "Geothermal (off-grid)",
    "Geothermal (total)": "Geothermal",
    "Hydropower": "Hydropower",
    # "Hydropower (off-grid)": "Hydropower (off-grid)",
    "Hydropower (incl. pumped storage)": "Hydropower (total)",
    # "Hydropower (excl. pumped storage)": "Hydropower (excl. pumped storage)",
    "Liquid biofuels": "Liquid biofuels",
    # "Liquid biofuels (off-grid)": "Liquid biofuels (off-grid)",
    "Marine": "Marine",
    "Mixed hydro plants": "Mixed hydro plants",
    "Offshore wind": "Offshore wind",
    "Onshore wind": "Onshore wind",
    # "Onshore wind (off-grid)": "Onshore wind (off-grid)",
    "Pumped storage": "Pumped storage",
    "Renewable municipal waste": "Renewable municipal waste",
    "Renewable electricity": "All renewables (total)",
    "Solar": "Solar (total)",
    "Solar photovoltaic": "Solar photovoltaic",
    "Solar photovoltaic (off-grid)": "Solar photovoltaic (off-grid)",
    "Solid biofuels": "Solid biofuels",
    # "Solid biofuels (off-grid)": "Solid biofuels (off-grid)",
    "Wind": "Wind (total)",
}


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
    tb = tb.rename(columns={variable: tb[variable].metadata.display["name"] for variable in tb.columns}, errors="raise")

    # Select and rename columns.
    tb = tb[TECHNOLOGIES.keys()].rename(columns=TECHNOLOGIES, errors="raise")

    # The original table has a column per technology, each with it's own short description.
    # I will gather all descriptions and add them later as a key description of the resulting (melted) capacity indicator.
    short_descriptions = {column: tb[column].metadata.description_short for column in tb.columns}

    # Simplify table to consider only the World.
    # Here we use "country" to refer to a technology.
    # This is a workaround, so that grapher will let us select technologies as it does with countries.
    tb = tb.loc["World"].reset_index().melt(id_vars="year", var_name="country", value_name="capacity")

    # Convert units from megawatts to gigawatts.
    tb["capacity"] *= MW_TO_GW

    # Set appropriate metadata.
    tb["capacity"].metadata.title = "Capacity"
    from owid.catalog import VariablePresentationMeta

    tb["capacity"].metadata.presentation = VariablePresentationMeta(
        title_public="Installed capacity for different renewable technologies"
    )
    # IRENA's data is rounded to 1 MW, with anything below 0.5 MW shown as 0.
    # tb["capacity"].metadata.display = {"numDecimalPlaces": 0}
    tb["capacity"].metadata.unit = "gigawatts"
    tb["capacity"].metadata.short_unit = "GW"
    tb["capacity"].metadata.description_short = "Measured in gigawatts."
    tb["capacity"].metadata.description_key = [
        f"{technology}: {description}" for technology, description in short_descriptions.items()
    ]

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
