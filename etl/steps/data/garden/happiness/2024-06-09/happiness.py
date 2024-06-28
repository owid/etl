"""Load a meadow dataset and create a garden dataset."""
import owid.catalog.processing as pr
from owid.catalog import Dataset, Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


ALL_REGIONS = {reg: reg_dict for reg, reg_dict in geo.REGIONS.items() if reg != "European Union (27)"}
ALL_REGIONS.update({"World": {}})


def remove_regions_below_population_threshold(
    tb: Table, regions: dict, ds_population: Dataset, threshold: float
) -> Table:
    """
    Check the share of population covered by the regions in the table.
    """
    msk = tb["country"].isin(regions.keys())
    tb_region = tb[msk]
    tb_no_regions = tb[~msk]
    tb_region = geo.add_population_to_table(tb_region, ds_population, population_col="total_population")
    tb_region["share_population"] = tb_region["population"] / tb_region["total_population"]
    tb_region = tb_region[tb_region["share_population"] >= threshold]
    tb_region = tb_region.drop(columns=["total_population", "share_population"])
    tb = pr.concat([tb_region, tb_no_regions])
    return tb


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load datasets: meadow dataset (latest happiness report), previous years, happiness by ages
    # for regional aggregates: population dataset, regions dataset, income groups dataset
    ds_meadow = paths.load_dataset("happiness", version="2024-06-09")
    ds_prev_years = paths.load_dataset("happiness", channel="garden", version="2023-03-20")
    ds_happiness_ages = paths.load_dataset("happiness_ages")

    ds_population = paths.load_dataset("population", channel="garden")
    ds_regions = paths.load_dataset("regions")
    ds_income_groups = paths.load_dataset("income_groups")

    # Read table datasets.
    tb_this_year = ds_meadow["happiness"].reset_index()
    tb_prev_years = ds_prev_years["happiness"]

    # combine meadow data with previous years
    tb_this_year["cantril_ladder_score"] = tb_this_year["ladder_score"]
    cols_overlap = ["country", "cantril_ladder_score", "year"]
    tb = pr.concat([tb_this_year[cols_overlap], tb_prev_years], ignore_index=True)

    # Read table including happiness by age group
    tb_ages = ds_happiness_ages["happiness_ages"].reset_index()

    # Harmonize country names
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb_ages = geo.harmonize_countries(
        df=tb_ages, countries_file=paths.country_mapping_path, warn_on_unused_countries=False
    )
    # Process happiness by age group data
    # drop unneeded columns from age table
    tb_ages = tb_ages.drop(
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
    tb_ages["age_group"] = tb_ages["age_group"].str.replace("Age ", "")

    # standardize happiness by age group and happiness data
    tb["age_group"] = "all ages"
    tb["happiness_score"] = tb["cantril_ladder_score"]
    tb = tb.drop(columns=["cantril_ladder_score"])

    #
    # Add population weighted averages for continents & income groups)
    #
    # save data of Northern Cyrpus and Somaliland to concat later (they do not have population in population dataset)
    countries_no_pop_msk = tb["country"].isin(["Northern Cyprus", "Somaliland"])
    tb_countries_wo_population = tb[countries_no_pop_msk]
    tb = tb[~countries_no_pop_msk]

    # add population to tables
    tb = geo.add_population_to_table(tb, ds_population)
    tb_ages = geo.add_population_to_table(tb_ages, ds_population)

    # calculate population weighted averages by multiplying the population with the cantril ladder score
    # and then summing and dividing by the total population
    tb["happiness_times_pop"] = tb["happiness_score"] * tb["population"]
    tb_ages["happiness_times_pop"] = tb_ages["happiness_score"] * tb_ages["population"]

    # set population to NaN where happiness_score is NaN
    tb_ages["population"] = tb_ages["population"].where(~tb_ages["happiness_score"].isna(), other=None)

    aggr_score = {"happiness_times_pop": "sum", "population": "sum"}
    tb = geo.add_regions_to_table(
        tb,
        aggregations=aggr_score,
        regions=ALL_REGIONS,
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        index_columns=["country", "year", "age_group"],
        min_num_values_per_year=1,
    )

    tb_ages = geo.add_regions_to_table(
        tb_ages,
        aggregations=aggr_score,
        regions=ALL_REGIONS,
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        index_columns=["country", "year", "age_group"],
        min_num_values_per_year=1,
    )

    # For happiness by age group, remove all regions where less than 50% of the population is covered
    tb_ages = remove_regions_below_population_threshold(tb_ages, ALL_REGIONS, ds_population, threshold=0.5)

    # Divide the sum of the cantril ladder score times population by the total population
    # concatenate the two tables
    tb = pr.concat([tb, tb_ages], ignore_index=True)
    tb["happiness_score"] = tb["happiness_times_pop"] / tb["population"]

    # drop unneeded columns
    tb = tb.drop(columns=["happiness_times_pop"])

    # add back Northern Cyprus and Somaliland
    tb = pr.concat([tb, tb_countries_wo_population], ignore_index=True)

    # drop population
    tb = tb.drop(columns=["population"])

    tb = tb.format(["country", "year", "age_group"])

    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the dataset
    ds_garden.save()
