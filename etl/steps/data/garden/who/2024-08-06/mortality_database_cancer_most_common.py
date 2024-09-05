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
    tb = ds_meadow.read_table("mortality_database_cancer")

    #
    # Process data.
    #
    tb_cleaned = tb.dropna(subset=["age_standardized_death_rate_per_100_000_standard_population"])

    # Group by 'country', 'year', 'sex', and 'age_group' and find the cause with the maximum death rate
    most_deadly_causes = tb_cleaned.loc[
        tb_cleaned.groupby(["country", "year", "sex", "age_group"])[
            "age_standardized_death_rate_per_100_000_standard_population"
        ].idxmax()
    ]

    # Keep only the 'cause' column
    most_deadly_causes = most_deadly_causes[["country", "year", "sex", "cause"]]

    tb = most_deadly_causes.format(["country", "year", "sex"])
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=False, default_metadata=ds_meadow.metadata
    )
    # Save changes in the new garden dataset.
    ds_garden.save()
