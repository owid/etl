"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("vaccine_preventable_incidence")
    ds_intro = paths.load_dataset("vaccination_schedules")

    # Read table from meadow dataset.
    tb = ds_meadow.read("vaccine_preventable_incidence")
    tb_intro = ds_intro.read("vaccination_schedules")

    #
    # Process data.
    #
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    tb_intro = calculate_years_from_vaccine_introduction(tb, tb_intro)
    tb = tb.drop(columns=["group", "code", "disease"])
    tb = tb.format(["country", "year", "disease_description", "denominator"])
    tb_intro = tb_intro.format(
        ["country", "year", "disease_description", "denominator"], short_name="years_since_vaccine_introduction"
    )

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb, tb_intro], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def find_first_year(tb_intro: Table) -> Table:
    """Find the first year the vaccination is introduced for each disease i.e."""

    filtered_tb = tb_intro[
        tb_intro["intro"].isin(
            [
                "Entire country",
                "Specific risk groups",
                "Regions of the country",
                "High risk areas",
                "Adolescents",
                # "Not routinely administered",
                # "During outbreaks",
                # "Demonstration projects",
            ]
        )
    ]
    first_year_tb = filtered_tb.groupby(["country", "description"])["year"].min().reset_index()
    first_year_tb = first_year_tb.rename(columns={"year": "first_year"})

    return first_year_tb


def calculate_years_from_vaccine_introduction(tb: Table, tb_intro: Table) -> Table:
    """Calculate the years from the introduction of the vaccine for each country and disease."""

    vaccine_disease_dict = {
        "Measles-containing vaccine 2nd dose": "Measles",
        "aP (acellular pertussis) vaccine": "Pertussis",
        "IPV (Inactivated polio vaccine)": "Polio",
        "IPV (Inactivated polio vaccine) 2nd dose": "Polio",
        "YF (Yellow fever) vaccine": "Yellow fever",
        "Rubella vaccine": "Rubella",
        "Japanese Encephalitis": "Japanese encephalitis",
        "Typhoid vaccine": "Typhoid",
        "Meningococcal meningitis vaccines (all strains)": "Invasive meningococcal disease",
    }

    first_year_tb = find_first_year(tb_intro)
    first_year_tb["disease_description"] = first_year_tb["description"].replace(vaccine_disease_dict)

    tb = tb.merge(first_year_tb, on=["country", "disease_description"], how="inner")
    tb["years_from_introduction"] = tb["year"] - tb["first_year"]
    tb = tb.drop(columns=["first_year", "description", "year", "group", "code", "disease"])
    tb = tb.rename(columns={"years_from_introduction": "year"})

    return tb
