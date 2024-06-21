from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

REGIONS = {reg: reg_dict for reg, reg_dict in geo.REGIONS.items() if reg != "European Union (27)"}
REGIONS.update({"World": {}})


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("happiness_ages")

    # load datasets for aggregation
    ds_population = paths.load_dataset("population", channel="garden")
    ds_regions = paths.load_dataset("regions")
    ds_income_groups = paths.load_dataset("income_groups")

    # Read table from meadow dataset.
    tb = ds_meadow["happiness_ages"].reset_index()

    #
    # Process data.
    #
    # drop unneeded columns:
    tb = tb.drop(
        columns=[
            "region",
            "age_group_code",
            "stress_score",
            "worry_score",
            "happiness_count",
            "stress_count",
            "worry_count",
        ]
    )

    # remove leading "Age " from age_group
    tb["age_group"] = tb["age_group"].str.replace("Age ", "")

    # add regional aggregates
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    tb = geo.add_population_to_table(tb, ds_population)

    tb["happiness_times_pop"] = tb["happiness_score"] * tb["population"]

    aggr_score = {"happiness_times_pop": "sum", "population": "sum"}
    tb = geo.add_regions_to_table(
        tb,
        aggregations=aggr_score,
        regions=REGIONS,
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        index_columns=["country", "year", "age_group"],
        min_num_values_per_year=1,
    )
    tb["happiness_score"] = tb["happiness_times_pop"] / tb["population"]

    tb = tb.drop(columns=["happiness_times_pop"])

    tb = tb.format(["country", "year", "age_group"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
