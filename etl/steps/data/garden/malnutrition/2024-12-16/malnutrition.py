from owid.catalog import processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

COLUMNS = {
    "sh_sta_stnt_me_zs": "number_of_stunted_children",
    "sh_sta_maln_zs": "number_of_underweight_children",
    "sh_sta_wast_zs": "number_of_wasted_children",
}


def run(dest_dir: str) -> None:
    paths.log.info("start")
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("wdi")
    ds_population = paths.load_dataset("un_wpp")

    # Read table from meadow dataset.
    tb = ds_meadow["wdi"].reset_index()
    tb = tb[["country", "year"] + list(COLUMNS.keys())]
    # Get the under-five population data.
    tb_population = ds_population.read("population", reset_metadata=True)
    tb_under_five = tb_population[
        (tb_population["age"] == "0-4") & (tb_population["sex"] == "all") & (tb_population["variant"] == "estimates")
    ].drop(columns=["population_change", "population_density"])
    # Merge the two datasets.
    tb = pr.merge(tb, tb_under_five, on=["country", "year"])

    # Calculate the number of malnourished children.
    for col in COLUMNS.keys():
        tb[COLUMNS[col]] = ((tb[col] / 100) * tb["population"]).round(0).astype("Int64")

    # Drop the columns that are no longer needed.
    tb = tb.drop(columns=list(COLUMNS.keys()) + ["population", "sex", "age", "variant"])
    tb = tb.dropna(subset=[COLUMNS[col] for col in COLUMNS.keys()], how="all")
    # Format
    tb = tb.format(["country", "year"], short_name="malnutrition")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()

    paths.log.info("end")
