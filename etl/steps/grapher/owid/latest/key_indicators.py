from copy import deepcopy

from owid import catalog

from etl import grapher_helpers as gh
from etl.paths import DATA_DIR

KEY_INDICATORS_GARDEN = DATA_DIR / "garden/owid/latest/key_indicators"


def run(dest_dir: str) -> None:
    # NOTE: this generates shortName `population_density__owid_latest`, perhaps we should keep it as `population_density`
    # and create unique constraint on (shortName, version, namespace) instead of just (shortName, namespace)
    garden_dataset = catalog.Dataset(KEY_INDICATORS_GARDEN)

    # Temporary sources metadata
    # garden_dataset.metadata.sources = [
    #     catalog.Source(name="Gapminder (v6)", published_by="Gapminder (v6)", url="https://www.gapminder.org/data/documentation/gd003/", date_accessed="October 8, 2021"),
    #     catalog.Source(name="UN", published_by="United Nations - Population Division (2022)", url="https://population.un.org/wpp/Download/Standard/Population", date_accessed="October 8, 2021"),
    #     catalog.Source(name="HYDE (v3.2)", published_by="HYDE (v3.2)", url="https://dataportaal.pbl.nl/downloads/HYDE/", date_accessed="September 9, 2022"),
    # ]
    print("_____________________")
    print(garden_dataset.metadata)
    print("_____________________")
    dataset = catalog.Dataset.create_empty(dest_dir, gh.adapt_dataset_metadata_for_grapher(garden_dataset.metadata))

    # Add population table
    table = garden_dataset["population"].reset_index()
    # table["something"] = table["population"]
    table = gh.adapt_table_for_grapher(table)
    dataset.add(table)

    # Add land area table
    table = gh.adapt_table_for_grapher(garden_dataset["land_area"].reset_index())
    dataset.add(table)
    
    dataset.save()
    print("_____________________")
    print(dataset.metadata)
    print("_____________________")