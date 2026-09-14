"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Metrics expected in the snapshot, and how their central value and bounds are named in the output.
METRICS = [
    "share_of_commercial_hens_sexed_in_ovo",
    "share_of_all_hens_sexed_in_ovo",
    "hens_sexed_in_ovo",
    "female_chicks_produced_with_in_ovo_sexing",
    "cumulative_male_embryos_removed",
]


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("in_ovo_sexing_market_penetration")
    tb = ds_meadow.read("in_ovo_sexing_market_penetration")

    #
    # Process data.
    #
    sanity_check_inputs(tb=tb)

    # Reshape from one row per metric to one column per metric (with separate columns for the bounds of the range).
    tb = tb.pivot(
        index=["country", "date"],
        columns="metric",
        values=["value", "value_low", "value_high"],
        join_column_levels_with="__",
        fill_dimensions=False,
    )
    tb = tb.rename(
        columns={
            f"{prefix}__{metric}": metric + suffix
            for metric in METRICS
            for prefix, suffix in [("value", ""), ("value_low", "_low"), ("value_high", "_high")]
        },
        errors="raise",
    )
    # Drop bound columns that are empty for all rows.
    tb = tb.dropna(axis=1, how="all")

    # Convert millions to units, and round away the float32 noise introduced when the meadow table was stored.
    for column in tb.drop(columns=["country", "date"]).columns:
        if column.startswith("share_"):
            tb[column] = tb[column].astype("Float64").round(1)
        else:
            tb[column] = (tb[column].astype("Float64") * 1e6).round(-2)

    # Improve table format.
    tb = tb.format(keys=["country", "date"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb])

    # Save changes in the new garden dataset.
    ds_garden.save()


def sanity_check_inputs(tb: Table) -> None:
    error = f"Unexpected metrics in the snapshot: {set(tb['metric']) - set(METRICS)}"
    assert set(tb["metric"]) == set(METRICS), error

    error = "Shares should be between 0 and 100."
    shares = tb[tb["metric"].str.startswith("share_")]
    values = shares[["value", "value_low", "value_high"]].fillna(0)
    assert ((values >= 0) & (values <= 100)).all().all(), error

    error = "Ranges should contain the central value."
    ranges = tb.dropna(subset=["value", "value_low", "value_high"])
    assert ((ranges["value_low"] <= ranges["value"]) & (ranges["value"] <= ranges["value_high"])).all(), error

    error = "The cumulative series should be non-decreasing."
    cumulative = tb[tb["metric"] == "cumulative_male_embryos_removed"].sort_values("date")
    assert cumulative["value"].is_monotonic_increasing, error
