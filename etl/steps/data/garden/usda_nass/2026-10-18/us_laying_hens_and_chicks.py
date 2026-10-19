"""Load a meadow dataset and create a garden dataset.

The monthly series are added up into annual totals following USDA's own definitions: the layer series cover December of
the previous year through November (the period of USDA's annual estimates for layers and eggs), and egg-type chicks
hatched cover the calendar year.
"""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Data items in the meadow table, and the name of the corresponding indicator.
DATA_ITEMS = {
    "CHICKENS, LAYERS - SALES FOR SLAUGHTER, MEASURED IN HEAD": "layers_sold_for_slaughter",
    "CHICKENS, LAYERS - LOSS, DEATH & RENDERED, MEASURED IN HEAD": "layers_lost",
    "CHICKENS, CHICKS, EGG TYPE - HATCHED, MEASURED IN HEAD": "egg_type_chicks_hatched",
    "CHICKENS, LAYERS - INVENTORY, AVG, MEASURED IN HEAD": "average_layers",
    "CHICKENS, LAYERS, TABLE - INVENTORY, AVG, MEASURED IN HEAD": "average_table_egg_layers",
    "CHICKENS, LAYERS, HATCHING, BROILER TYPE - INVENTORY, AVG, MEASURED IN HEAD": "average_broiler_type_hatching_egg_layers",
    "CHICKENS, LAYERS, HATCHING, EGG TYPE - INVENTORY, AVG, MEASURED IN HEAD": "average_egg_type_hatching_egg_layers",
    "CHICKENS, YOUNG, SLAUGHTER, FI - SLAUGHTERED, MEASURED IN HEAD": "young_chickens_slaughtered",
    "CHICKENS, MATURE, SLAUGHTER, FI - SLAUGHTERED, MEASURED IN HEAD": "mature_chickens_slaughtered",
    "CHICKENS, MATURE, LIGHT, SLAUGHTER, FI - SLAUGHTERED, MEASURED IN HEAD": "light_mature_chickens_slaughtered",
    "CHICKENS, MATURE, HEAVY, SLAUGHTER, FI - SLAUGHTERED, MEASURED IN HEAD": "heavy_mature_chickens_slaughtered",
    "CHICKENS, SLAUGHTER, FI - SLAUGHTERED, MEASURED IN HEAD": "chickens_slaughtered",
}
# Layer series by type, published only as monthly averages, which are averaged into annual values.
LAYERS_BY_TYPE = [
    "average_table_egg_layers",
    "average_broiler_type_hatching_egg_layers",
    "average_egg_type_hatching_egg_layers",
]
# Layer series, whose year runs from December of the previous year through November.
LAYER_SERIES = ["layers_sold_for_slaughter", "layers_lost", "average_layers"] + LAYERS_BY_TYPE
# Monthly series added up into annual totals.
SUMMED_SERIES = ["layers_sold_for_slaughter", "layers_lost", "egg_type_chicks_hatched"]
MONTHS = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
# Annual series taken as published, and their period: the average number of layers covers December of the previous year
# through November, and the slaughter series cover the calendar year.
ANNUAL_PERIODS = {
    "average_layers": "MARKETING YEAR",
    "young_chickens_slaughtered": "YEAR",
    "mature_chickens_slaughtered": "YEAR",
    "light_mature_chickens_slaughtered": "YEAR",
    "heavy_mature_chickens_slaughtered": "YEAR",
    "chickens_slaughtered": "YEAR",
}


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("us_laying_hens_and_chicks")
    tb = ds_meadow.read("us_laying_hens_and_chicks")

    #
    # Process data.
    #
    sanity_check_inputs(tb=tb)

    # Name each data item.
    tb["indicator"] = tb["data_item"].map(DATA_ITEMS)

    # Monthly values, with the year they count towards: for the layer series, December counts towards the following year.
    tb_monthly = tb[tb["frequency"] == "MONTHLY"].reset_index(drop=True)
    tb_monthly["month"] = tb_monthly["period"].map({month: i for i, month in enumerate(MONTHS, start=1)})
    december_of_layer_series = tb_monthly["indicator"].isin(LAYER_SERIES) & (tb_monthly["month"] == 12)
    tb_monthly.loc[december_of_layer_series, "year"] += 1

    # Add up the monthly flows, and average the monthly layer inventories by type, keeping only complete years.
    tb_summed = tb_monthly[tb_monthly["indicator"].isin(SUMMED_SERIES)]
    tb_summed = tb_summed.groupby(["indicator", "year"], as_index=False).agg(
        value=("value", "sum"), month=("month", "count")
    )
    tb_averaged = tb_monthly[tb_monthly["indicator"].isin(LAYERS_BY_TYPE)]
    tb_averaged = tb_averaged.groupby(["indicator", "year"], as_index=False).agg(
        value=("value", "mean"), month=("month", "count")
    )
    tb_averaged["value"] = tb_averaged["value"].round().astype("Int64")
    tb_annual = pr.concat([tb_summed, tb_averaged], ignore_index=True)
    tb_annual = tb_annual[tb_annual["month"] == 12].drop(columns="month").reset_index(drop=True)

    # Annual series taken as published.
    tb_published = tb[(tb["frequency"] == "ANNUAL") & (tb["period"] == tb["indicator"].map(ANNUAL_PERIODS))]
    tb_published = tb_published[["indicator", "year", "value"]].reset_index(drop=True)

    # Combine all annual series in a wide table.
    tb = pr.concat([tb_annual, tb_published], ignore_index=True)
    tb = tb.pivot(index="year", columns="indicator", values="value", join_column_levels_with="_")
    tb["country"] = "United States"

    # Correct known errors in the source data.
    tb = paths.apply_corrections(tb)

    sanity_check_outputs(tb=tb, tb_monthly=tb_monthly)

    # Improve table format.
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()


def sanity_check_inputs(tb: Table) -> None:
    error = "Unexpected data items."
    assert set(tb["data_item"]) == set(DATA_ITEMS), error
    error = "Unexpected frequencies or periods."
    assert set(tb["frequency"]) == {"MONTHLY", "ANNUAL"}, error
    assert set(tb[tb["frequency"] == "MONTHLY"]["period"]) == set(MONTHS), error
    assert set(tb[tb["frequency"] == "ANNUAL"]["period"]) == set(ANNUAL_PERIODS.values()), error
    error = "All values should be positive numbers."
    assert tb["value"].notna().all() and (tb["value"] > 0).all(), error


def sanity_check_outputs(tb: Table, tb_monthly: Table) -> None:
    error = "Unexpected columns."
    assert set(tb.columns) == {"country", "year"} | set(DATA_ITEMS.values()), error
    error = "Annual series should have no gaps."
    for column in DATA_ITEMS.values():
        years = tb[tb[column].notna()]["year"]
        assert years.max() - years.min() + 1 == len(years), error

    # The annual average number of layers should be the mean of the monthly averages from December through November.
    monthly_average = tb_monthly[tb_monthly["indicator"] == "average_layers"]
    monthly_average = monthly_average.groupby("year")["value"].agg(["mean", "count"])
    monthly_average = monthly_average[monthly_average["count"] == 12]["mean"]
    annual_average = tb.set_index("year")["average_layers"]
    ratio = (annual_average / monthly_average).dropna()
    error = "The annual average number of layers does not match the mean of the monthly averages."
    assert len(ratio) > 10 and ((ratio - 1).abs() < 0.01).all(), error

    # Young plus mature chickens slaughtered should add up to all chickens slaughtered, and light plus heavy mature
    # chickens to all mature chickens.
    for parts, total in [
        (["young_chickens_slaughtered", "mature_chickens_slaughtered"], "chickens_slaughtered"),
        (["light_mature_chickens_slaughtered", "heavy_mature_chickens_slaughtered"], "mature_chickens_slaughtered"),
    ]:
        residual = tb[parts].sum(axis=1, min_count=len(parts)) - tb[total]
        known = residual.notna()
        error = f"{parts} do not add up to {total}."
        assert known.sum() > 10 and (residual[known].abs() <= 1e-4 * tb.loc[known, total]).all(), error

    # Table egg layers plus hatching egg layers of both types should add up to all layers (within rounding).
    residual = (tb[LAYERS_BY_TYPE].sum(axis=1, min_count=len(LAYERS_BY_TYPE)) / tb["average_layers"] - 1).dropna()
    error = "Layers by type do not add up to all layers."
    assert len(residual) > 10 and (residual.abs() < 0.01).all(), error

    error = "Indicators should carry the origin of the snapshot."
    assert all(len(tb[column].metadata.origins) == 1 for column in DATA_ITEMS.values()), error
