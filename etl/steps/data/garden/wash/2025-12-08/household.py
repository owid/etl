"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("household")
    snap_definition = paths.load_snapshot("definitions")

    # Read table from meadow dataset.
    tb = ds_meadow.read("household_countries")
    tb_reg = ds_meadow.read("household_regions")
    tb_def = snap_definition.read_excel(sheet_name="variables")

    # combine region and region type into a single column
    tb_reg["country"] = tb_reg["region"] + " (" + tb_reg["region_type"] + ")"
    tb_reg = tb_reg.drop(columns=["region", "region_type"])

    # rename hyg_ns to hyg_nfac in regions table to match countries table
    tb_reg = tb_reg.rename(columns={"hyg_ns": "hyg_nfac"})

    tb = pr.concat([tb, tb_reg], ignore_index=True)

    tb = tb.drop(columns=["iso3"])

    ## add metadata to columns
    ## this metadata comes from the definitions sheet provided by WHO/UNICEF - it provides a default that gets finetuned in the garden metadata .yml file
    tb = add_metadata_to_columns(tb, tb_def)

    # add populations for each indicator
    # population is given in thousands
    tb["pop"] = tb["pop"] * 1000

    # calculate indicators as population
    tb = calculate_population_all_indicators(tb)

    # calculate population without service for selected indicators
    service_cols = ["wat_basal", "wat_imp", "wat_sm", "san_imp", "san_sm", "hyg_bas"]
    tb = calculate_population_without_service(tb, service_cols)

    # drop population column
    tb = tb.drop(columns=["pop"])

    #
    # Process data.
    #
    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb=tb)

    # Improve table format.
    tb = tb.format(["country", "year", "residence"], short_name="household")

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()


def calculate_population_all_indicators(tb):
    """
    Calculate the population for all indicators.

    Multiply each indicator column by the 'pop' column and append '_pop' to the column name.

    """
    columns = tb.columns.drop(["country", "year", "pop", "residence"])

    for col in columns:
        tb[f"{col}_pop"] = (tb[col] / 100) * tb["pop"]
        # add metadata to new column
        title_col = tb[col].metadata.title if tb[col].metadata.title else col
        if "Proportion of " in title_col:
            new_title = title_col.split("Proportion of ", 1)[1].capitalize()
        else:
            new_title = title_col
        tb[f"{col}_pop"].metadata.title = new_title
        tb[f"{col}_pop"].metadata.description_from_producer = tb[col].metadata.description_from_producer
        tb[f"{col}_pop"].metadata.unit = "people"
        tb[f"{col}_pop"].metadata.short_unit = ""
        tb[f"{col}_pop"].metadata.update(display={"numDecimalPlaces": 0})

    return tb


def calculate_population_without_service(tb, service_cols):
    """
    Calculate the population _without_ given services for a selection of services, that we show in charts.

    """
    for col in service_cols:
        tb[f"{col}_without"] = 100 - tb[col]
        tb[f"{col}_pop_without"] = (tb[f"{col}_without"] / 100) * tb["pop"]
        # add metadata to new column
        if col == "wat_basal":
            tb[f"{col}_pop_without"].metadata.title = "Population without basic drinking water service"
        elif col == "wat_imp":
            tb[f"{col}_pop_without"].metadata.title = "Population without improved drinking water service"
        elif col == "wat_sm":
            tb[f"{col}_pop_without"].metadata.title = "Population without safely managed drinking water service"
        elif col == "san_imp":
            tb[f"{col}_pop_without"].metadata.title = "Population without improved sanitation service"
        elif col == "san_sm":
            tb[f"{col}_pop_without"].metadata.title = "Population without safely managed sanitation service"
        elif col == "hyg_bas":
            tb[f"{col}_pop_without"].metadata.title = "Population without basic hygiene service"
        tb[f"{col}_pop_without"].metadata.description_from_producer = tb[col].metadata.description_from_producer
        tb[f"{col}_pop_without"].metadata.unit = "people"
        tb[f"{col}_pop_without"].metadata.short_unit = ""
        tb[f"{col}_pop_without"].metadata.update(display={"numDecimalPlaces": 0})

    return tb


def add_metadata_to_columns(tb, tb_def):
    """
    Add metadata to columns from the definitions table.

    """
    for _, row in tb_def.iterrows():
        # column name is varname without last _
        column_name = row["varname"].rsplit("_", 1)[0]
        if column_name in tb.columns:
            tb[column_name].metadata.title = row["label_long"]
            tb[column_name].metadata.description_from_producer = row["definition"]
            tb[column_name].metadata.unit = "percent"
            tb[column_name].metadata.short_unit = "%"
            tb[column_name].metadata.update(display={"numDecimalPlaces": 1})

    return tb
