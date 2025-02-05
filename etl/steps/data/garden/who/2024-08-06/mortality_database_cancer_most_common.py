"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("mortality_database_cancer")
    tb = ds_meadow.read("mortality_database_cancer")

    #
    # Process data.
    #
    # Keep only the columns relevant for age-standardized death rate per 100,000 standard population which we'll use to get the most deadly cancers
    tb = tb[["country", "year", "sex", "cause", "age_standardized_death_rate_per_100_000_standard_population"]]

    # Remove rows with missing values in 'age_standardized_death_rate_per_100_000_standard_population'
    tb = tb.dropna(subset=["age_standardized_death_rate_per_100_000_standard_population"])
    # Group by 'country', 'year', 'sex', and 'age_group' and find the cause with the maximum death rate
    tb = tb.loc[
        tb.groupby(["country", "year", "sex"])["age_standardized_death_rate_per_100_000_standard_population"].idxmax()
    ]

    # Replace "cancer" or "cancers" with an empty string, and replace commas with "and"
    tb["cause"] = (
        tb["cause"].str.replace(r"\bcancers?\b", "", case=False, regex=True).str.replace(",", " and").str.strip()
    )
    tb = tb[["country", "year", "sex", "cause"]]

    tb = tb.format(["country", "year", "sex"])
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=False, default_metadata=ds_meadow.metadata
    )
    # Save changes in the new garden dataset.
    ds_garden.save()
