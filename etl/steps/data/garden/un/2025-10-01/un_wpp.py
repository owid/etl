# from deaths import process as process_deaths
# from demographics import process as process_demographics
# from dep_ratio import process as process_depratio
# from fertility import process as process_fertility

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

YEAR_SPLIT = 2024
COLUMNS_INDEX = ["country", "year", "sex", "age", "variant"]
COLUMNS_INDEX_MONTH = COLUMNS_INDEX + ["month"]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("un_wpp")

    # Load tables
    tb_population = ds_meadow.read("population")

    tb_population = tb_population[tb_population["month"] == "July"]
    tb_population = tb_population.drop(columns="month")
    #
    # Process data.

    tb_population = geo.harmonize_countries(
        tb_population, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    tb_population = tb_population.format(COLUMNS_INDEX)
    # Build tables list for dataset
    tables = [tb_population]

    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=tables,
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
