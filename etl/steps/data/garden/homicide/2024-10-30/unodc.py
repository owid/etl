"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table
from owid.catalog.utils import underscore

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("unodc")

    # Read table from meadow dataset.
    tb = ds_meadow["unodc"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    tb = clean_up_categories(tb)
    tables = clean_data(tb)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=tables, check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def clean_data(tb: Table) -> list[Table]:
    """
    Splitting the data into four dataframes/tables based on the dimension columns:
    * Total
    * by mechanism
    * by relationship to perpertrator
    * by situational context
    """
    tb_mech = create_table(tb, table_name="by mechanisms")
    tb_perp = create_table(tb, table_name="by relationship to perpetrator")
    tb_situ = create_table(tb, table_name="by situational context")
    tb_tot = create_total_table(tb)

    tb_garden_list = [tb_mech, tb_tot, tb_perp, tb_situ]

    return tb_garden_list


def create_table(tb: Table, table_name: str) -> Table:
    """
    Create the homicides by mechanism dataframe where we will have  homicides/homicide rate
    disaggregated by mechanism (e.g. weapon)

    """
    assert any(tb["dimension"] == table_name), "table_name must be a dimension in df"
    tb_filter = tb[tb["dimension"] == table_name]
    tb_filter = tb_filter.drop(columns=["indicator", "source", "dimension"])

    tb_filter = tb_filter.format(
        ["country", "year", "category", "sex", "age", "unit_of_measurement"],
        short_name=underscore(table_name),
    )

    return tb_filter


def create_total_table(tb: Table) -> Table:
    """
    Create the total homicides dataframe where we will have total homicides/homicide rate
    disaggregated by age and sex
    """
    tb_tot = tb[tb["dimension"] == "Total"]

    # There are some duplicates when sex is unknown so let's remove those rows
    tb_tot = tb_tot[tb_tot["sex"] != "Unknown"]

    tb_tot = tb_tot.drop(columns=["indicator", "source", "dimension"])

    tb_tot = tb_tot.format(
        ["country", "year", "category", "sex", "age", "unit_of_measurement"],
        short_name="total",
    )

    return tb_tot


def clean_up_categories(tb: Table) -> Table:
    """
    Make the categories used in the dataset a bit more readable.

    """
    category_dict = {
        "Another weapon - sharp object": "a sharp object",
        "Unspecified means": "unspecified means",
        "Without a weapon/ other Mechanism": " without a weapon or by another mechanism",
        "Firearms or explosives": "firearms or explosives",
        "Another weapon": "sharp or blunt object, including motor vehicles",
        "Intimate partner or family member": "Perpetrator is an intimate partner or family member",
        "Intimate partner or family member: Intimate partner": "Perpetrator is an intimate partner",
        "Intimate partner or family member: Family member": "Perpetrator is a family member",
        "Other Perpetrator known to the victim": "Another known perpetrator",
        "Perpetrator unknown to the victim": "Perpetrator unknown to victim",
        "Perpetrator to victim relationship unknown": "Perpetrator where the relationship to the victim is not known",
        "Socio-political homicide - terrorist offences": "Terrorist offences",
        "Unknown types of homicide": "Unknown situational context",
    }

    for key in category_dict.keys():
        assert key in tb["category"].values, f"{key} not in table"
    tb["category"] = tb["category"].cat.rename_categories({"category": category_dict})

    assert tb["category"].isna().sum() == 0
    return tb
