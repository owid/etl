"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its tables.
    ds_meadow = paths.load_dataset("natural_hazards")
    tb_earthquakes = ds_meadow["natural_hazards_earthquakes"].reset_index()
    tb_tsunamis = ds_meadow["natural_hazards_tsunamis"].reset_index()
    tb_volcanoes = ds_meadow["natural_hazards_volcanoes"].reset_index()

    #
    # Process data.
    #
    # The data on the socio-economic impacts is very sparse.
    # For example, the percentage of events that lack data on deaths is
    # 66% for earthquakes, 90% for tsunamis, and 50% for volcanoes.
    # len(tb_earthquakes[tb_earthquakes["deaths"].isnull()]) / len(tb_earthquakes) * 100
    # len(tb_tsunamis[tb_tsunamis["deaths"].isnull()]) / len(tb_tsunamis) * 100
    # len(tb_volcanoes[tb_volcanoes["deaths"].isnull()]) / len(tb_volcanoes) * 100

    # Harmonize country names.
    tb_earthquakes = geo.harmonize_countries(
        tb_earthquakes, countries_file=paths.country_mapping_path, warn_on_unused_countries=False
    )
    tb_tsunamis = geo.harmonize_countries(
        tb_tsunamis, countries_file=paths.country_mapping_path, warn_on_unused_countries=False
    )
    tb_volcanoes = geo.harmonize_countries(
        tb_volcanoes, countries_file=paths.country_mapping_path, warn_on_unused_countries=False
    )

    # Therefore, simply get data on number of events per country-year.
    # Also, remove duplicated events (we know at least one, event id 1926, for Chile 1961).
    tb_earthquakes_count = (
        tb_earthquakes.drop_duplicates(subset=["id"])
        .groupby(["country", "year"], observed=True, as_index=False)
        .agg({"id": "count"})
        .rename(columns={"id": "earthquakes"}, errors="raise")
    )
    tb_tsunamis_count = (
        tb_tsunamis.drop_duplicates(subset=["id"])
        .groupby(["country", "year"], observed=True, as_index=False)
        .agg({"id": "count"})
        .rename(columns={"id": "tsunamis"}, errors="raise")
    )
    tb_volcanoes_count = (
        tb_volcanoes.drop_duplicates(subset=["id"])
        .groupby(["country", "year"], observed=True, as_index=False)
        .agg({"id": "count"})
        .rename(columns={"id": "volcanoes"}, errors="raise")
    )

    # Merge the three tables.
    tb = pr.multi_merge(
        [tb_earthquakes_count, tb_tsunamis_count, tb_volcanoes_count], how="outer", on=["country", "year"]
    )

    # Format table conveniently.
    tb = tb.format(short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()
