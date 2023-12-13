"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table
from owid.catalog import processing as pr
from shared import add_variable_description_from_producer

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("laboratories")
    snap = paths.load_snapshot("data_dictionary.csv")
    ds_un_wpp = paths.load_dataset("un_wpp")

    ds_pop = ds_un_wpp["population"].reset_index()
    # Load data dictionary from snapshot.
    dd = snap.read()
    # Read table from meadow dataset.
    tb = ds_meadow["laboratories"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb = tb[["country", "year", "culture", "m_wrd"]]
    tb = add_variable_description_from_producer(tb, dd)
    tb = tb.dropna(subset=["culture", "m_wrd"], how="all")
    tb = add_population_and_rates(tb, ds_pop)
    tb = tb.set_index(["country", "year"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def add_population_and_rates(tb: Table, ds_pop: Table) -> Table:
    """
    Adding the total population of each country-year to the table and then calculating the rates per million people.

    """
    ds_pop = ds_pop[
        (ds_pop["variant"] == "estimates")
        & (ds_pop["age"] == "all")
        & (ds_pop["sex"] == "all")
        & (ds_pop["metric"] == "population")
    ]
    ds_pop = ds_pop.rename(columns={"location": "country", "value": "population"})

    tb_pop = pr.merge(tb, ds_pop, on=["country", "year"], how="left")
    tb_pop["culture_rate"] = (tb_pop["culture"] / tb_pop["population"]) * 1000000
    tb_pop["m_wrd_rate"] = (tb_pop["m_wrd"] / tb_pop["population"]) * 1000000
    # Converting to float16 to reduce warnings
    tb_pop[["culture_rate", "m_wrd_rate"]] = tb_pop[["culture_rate", "m_wrd_rate"]].astype("float16")
    tb_pop = tb_pop[["country", "year", "culture", "culture_rate", "m_wrd", "m_wrd_rate"]]

    return tb_pop
