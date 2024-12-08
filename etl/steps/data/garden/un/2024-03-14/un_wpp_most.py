from typing import Any

from owid.catalog import Table
from owid.catalog import processing as pr
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
log = get_logger()


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    ds_garden = paths.load_dataset("un_wpp")
    tb_pop = ds_garden["population"].reset_index()
    origins = tb_pop["population"].metadata.origins[0]
    age_group_size = [5, 10]
    tb_list = []
    tb_pop_filter = Table()
    for age_group in age_group_size:
        log.info(f"Creating population table for {age_group} year age groups")
        # filter data for just sex = all, metrics = population, variant = estimates
        if age_group == 5:
            tb_pop_filter = create_five_year_age_groups(tb_pop, origins)
        if age_group == 10:
            tb_pop_filter = create_ten_year_age_groups(tb_pop, origins)
        # Group by country and year, and apply the custom function
        tb_pop_filter = (
            tb_pop_filter.groupby(["country", "year"], group_keys=False)
            .apply(get_largest_age_group)
            .reset_index(drop=True)  # Reset index to have a clean DataFrame
        )
        # The function above creates NAs for some countrys that don't appear to be in the table e.g. Vatican, Melanesia, so dropping here

        tb_pop_filter = tb_pop_filter.drop(columns=["population"])
        tb_pop_filter = tb_pop_filter.set_index(["country", "year"], verify_integrity=True)
        tb_pop_filter.metadata.short_name = f"population_{age_group}_year_age_groups"
        tb_list.append(tb_pop_filter)
    # Save outputs.

    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=tb_list, default_metadata=ds_garden.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def create_ten_year_age_groups(tb: Table, origins: Any) -> Table:
    # Initialize an empty list to hold the age bands
    age_bands = []
    # Loop through a range with a step of 5, stopping before 100
    for i in range(20, 100, 10):
        age_bands.append(f"{i}-{i+9}")
    # Add the "100+" group at the end and 0-4 and 5-9 as 0-9 is not a group in the dataset
    age_bands = age_bands + ["100+", "0-4", "5-9", "10-14", "15-19"]
    # Filter the table to only include the age bands we want
    tb = tb[(tb.sex == "all") & (tb.variant == "estimates") & (tb.age.isin(age_bands))]
    assert tb["age"].nunique() == len(age_bands), "Age groups are not as expected"
    tb = tb.drop(columns=["sex", "variant", "population_change", "population_density"])

    # Create the 0-9 and 10-19 age groups
    tb_0_9 = tb[(tb.age == "0-4") | (tb.age == "5-9")]
    tb_0_9 = tb_0_9.groupby(["country", "year"], observed=False)["population"].sum().reset_index()
    tb_0_9["age"] = "0-9"

    tb_10_19 = tb[(tb.age == "10-14") | (tb.age == "15-19")]
    tb_10_19 = tb_10_19.groupby(["country", "year"], observed=False)["population"].sum().reset_index()
    tb_10_19["age"] = "10-19"
    # Drop the 0-4, 5-9, 10-14 and 15-19 age groups
    tb = tb[(tb.age != "0-4") & (tb.age != "5-9") & (tb.age != "10-14") & (tb.age != "15-19")]
    # Concatenate the 0-9 and 10-19 age groups with the original table
    tb = pr.concat([tb, tb_0_9, tb_10_19])
    tb = tb.rename(columns={"age": "age_group_ten"})
    tb["age_group_ten"].metadata.origins = [origins]
    tb = tb.reset_index(drop=True)
    return tb


def create_five_year_age_groups(tb: Table, origins: Any) -> Table:
    # Initialize an empty list to hold the age bands
    age_bands = []
    # Loop through a range with a step of 5, stopping before 100
    for i in range(0, 100, 5):
        age_bands.append(f"{i}-{i+4}")
    # Add the "100+" group at the end
    age_bands.append("100+")
    # Filter the table to only include the age bands we want
    tb = tb[(tb.sex == "all") & (tb.variant == "estimates") & (tb.age.isin(age_bands))]
    assert tb["age"].nunique() == len(age_bands), "Age groups are not as expected"
    tb = tb.drop(columns=["sex", "variant", "population_change", "population_density"])
    tb = tb.rename(columns={"age": "age_group_five"})
    tb["age_group_five"].metadata.origins = [origins]
    tb = tb.reset_index(drop=True)
    return tb


# Function to apply to each group to find the age group with the largest population
def get_largest_age_group(group):
    return group.loc[group["population"].idxmax()]
