"""Federico–Tena V2 population + Quality Assessment.

Two tables: the main population series at 1991 borders (harmonized) and the per-(country, year)
reliability classes (kept at the source's 1938 historical borders — country lists do not align
one-to-one with the main table, so we don't force harmonization here).
"""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("federico_tena_population")
    tb_pop = ds_meadow.read("federico_tena_population")
    tb_qa = ds_meadow.read("federico_tena_population_quality")

    #
    # Process data.
    #
    # Population table is at 1991 borders — harmonize against OWID's standard country list.
    # Unmapped entries are written to federico_tena_population.countries.json on first run.
    tb_pop = paths.regions.harmonize_names(
        tb=tb_pop,
        country_col="country",
        countries_file=paths.country_mapping_path,
    )
    tb_pop = tb_pop.format(["country", "year"], short_name="federico_tena_population")

    # Quality assessment table is at 1938 borders — keep source labels untouched for now.
    # Mapping these to 1991 borders is non-trivial (former colonies, partitioned states, etc.)
    # and we'd rather expose them as-is until the OMM consumer decides how to use them.
    tb_qa = tb_qa.format(["country", "year"], short_name="federico_tena_population_quality")

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb_pop, tb_qa], default_metadata=ds_meadow.metadata)
    ds_garden.save()
