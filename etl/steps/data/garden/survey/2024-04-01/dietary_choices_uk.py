"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

COLUMNS = {
    "Base": "base",
    "Unweighted base": "base_unweighted",
    "Flexitarian (mainly vegetarian, but occasionally eat meat or fish)": "flexitarian",
    "Meat eater (eat meat and/or poultry)": "meat_eater",
    "None of these": "none",
    "Pescetarian (eat fish but do not eat meat or poultry)": "pescetarian",
    "Plant-based / Vegan (do not eat dairy products, eggs, or any other animal product)": "vegan",
    "Vegetarian (do not eat any meat, poultry, game, fish or shellfish)": "vegetarian",
}


def run_sanity_checks(tb: Table) -> None:
    error = "Percentages do not add up to 100% for some of the surveyed dates (within 2%)."
    assert (abs(tb.drop(columns=["base", "base_unweighted"]).sum(axis=1) - 100) <= 2).all(), error

    error = "Negative values found in the table."
    assert (tb >= 0).all().all(), error

    error = "Base and unweighted base, on a given date, should add up to the same number (or at least within 1%)."
    _tb = tb.groupby(["date"]).agg({"base": "sum", "base_unweighted": "sum"})
    assert ((100 * abs(_tb["base"] - _tb["base_unweighted"]) / _tb["base_unweighted"]) < 1).all()


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("dietary_choices_uk")
    tb = ds_meadow["dietary_choices_uk"].reset_index()

    #
    # Process data.
    #
    # Rename diet column name for convenience.
    tb = tb.rename(columns={"which_of_these_best_describes_your_diet": "diet"}, errors="raise")

    # Rename diets.
    tb["diet"] = tb["diet"].map(COLUMNS)

    # Transform the table to long format.
    tb = tb.melt(id_vars=["diet", "group"], var_name="date", value_name="value")

    # Format date column.
    tb["date"] = tb["date"].str[1:].str.replace("_", "-")

    # Transform the table to wide format.
    tb = tb.pivot(index=["group", "date"], columns="diet", values="value", join_column_levels_with="_")

    # Convert fractions into percentages.
    tb[tb.drop(columns=["group", "date", "base", "base_unweighted"]).columns] *= 100

    # Ensure columns have the right type.
    tb = tb.astype({"base": int, "base_unweighted": int})

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(keys=["group", "date"], sort_columns=True)

    # Sanity checks on outputs.
    run_sanity_checks(tb=tb)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()
