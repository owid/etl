"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table
from shared import add_share_population

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
REGIONS = ["North America", "South America", "Europe", "Africa", "Asia", "Oceania"]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("gbd_mental_health_burden")
    # Read table from meadow dataset.
    tb = ds_meadow["gbd_mental_health_burden"].reset_index()
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Add a share of the population column
    tb = add_share_population(tb)
    #
    tb_daly = tb.query("measure == 'DALYs (Disability-Adjusted Life Years)'").drop(columns="measure")
    tb_daly = tb_daly.format(["country", "year", "cause", "metric", "age"], short_name="gbd_mental_health_burden_dalys")

    tb_entities = diseases_as_entities(tb)
    tb_entities = tb_entities.format(["country", "year"], short_name="gbd_mental_health_burden_entities")
    # fixing the metadata which has got lost somewhere

    for col in tb_entities.columns:
        print(col)
        tb_entities[col] = tb_entities[col].copy_metadata(tb["value"])
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[tb_entities, tb_daly],
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
        # Table has optimal types already and repacking can be time consuming.
        repack=False,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def diseases_as_entities(tb: Table) -> Table:
    """
    Save the diseases as entities, and measure as variable, for use in this chart: https://ourworldindata.org/grapher/estimated-prevalence-vs-burden-mental-illnesses
    """
    tb_ent = tb[(tb["country"] == "World") & (tb["age"] == "All ages")].copy()

    tb_ent = tb_ent.pivot_table(index=["cause", "year"], columns=["measure", "metric"], values="value").reset_index()
    tb_ent.columns = ["_".join(filter(None, col)).strip() for col in tb_ent.columns.values]
    tb_ent = tb_ent.rename(columns={"cause": "country"})
    tb_ent = tb_ent.drop(columns="DALYs (Disability-Adjusted Life Years)_Share")

    # tb_ent = tb_ent.copy_metadata(tb)
    return tb_ent
