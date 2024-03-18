from typing import List, cast

from owid.catalog import Dataset

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    ds_garden = cast(Dataset, paths.load_dependency("un_wpp"))
    tb_pop = ds_garden["population"].reset_index()

    # filter data for just sex = all, metrics = population, variant = estimates
    tb_pop = tb_pop[
        (tb_pop.sex == "all")
        & (tb_pop.metric == "population")
        & (tb_pop.variant == "estimates")
        & (tb_pop.age.isin(create_age_groups()))
    ]
    tb_pop = tb_pop.drop(columns=["metric", "sex", "variant"])
    assert tb_pop["age"].nunique() == 21
    # Group by country and year, and apply the custom function
    tb_pop = tb_pop.groupby(["location", "year"]).apply(get_largest_age_group)
    # The function above creates NAs for some locations that don't appear to be in the table e.g. Vatican, Melanesia, so dropping here
    tb_pop = tb_pop.dropna()
    tb_pop = tb_pop.reset_index(drop=True)
    tb_pop = tb_pop.set_index(["location", "year"], verify_integrity=True)
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_pop], default_metadata=ds_garden.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def create_age_groups() -> List[str]:
    # Initialize an empty list to hold the age bands
    age_bands = []

    # Loop through a range with a step of 5, stopping before 100
    for i in range(0, 100, 5):
        age_bands.append(f"{i}-{i+4}")

    # Add the "100+" group at the end
    age_bands.append("100+")

    return age_bands


# Function to apply to each group to find the age group with the largest population
def get_largest_age_group(group):
    return group.loc[group["value"].idxmax()]
