from owid.catalog import Table, Variable

from etl.helpers import PathFinder, create_dataset

paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    ds_gho = paths.load_dataset("gho")
    tb_gho = ds_gho.read("number_of_registered_vehicles").loc[:, ["country", "year", "number_of_registered_vehicles"]]

    ds_population = paths.load_dataset("population")
    tb_population = ds_population.read("population").loc[:, ["country", "year", "population"]]

    # Create and add table
    table = make_combined(tb_gho, tb_population)

    # Set an appropriate index and sort conveniently
    tb = table.format(["country", "year"], sort_columns=True)

    ds = create_dataset(dest_dir, tables=[tb])
    ds.save()


def make_combined(table_gho: Table, table_population: Table) -> Table:
    # Merge
    table = table_population.merge(table_gho, on=["country", "year"]).set_index(["country", "year"])

    # Calculate new variables
    table = table.assign(
        registered_vehicles_per_thousand=Variable(
            table.number_of_registered_vehicles / table.population * 1000,
            name="registered_vehicles_per_thousand",
        ),
    )
    # Filter columns and drop NA values
    table = table.loc[:, ["registered_vehicles_per_thousand"]].dropna(
        subset=["registered_vehicles_per_thousand"], how="all"
    )
    table.m.short_name = "vehicles"
    return table.reset_index()
