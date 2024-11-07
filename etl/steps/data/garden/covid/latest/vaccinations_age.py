"""Load a meadow dataset and create a garden dataset."""

from typing import List, Optional, cast

import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog.processing import concat
from shared import add_regions

from etl.data_helpers import geo
from etl.data_helpers.misc import expand_time_column, interpolate_table
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("vaccinations_age")

    # Read table from meadow dataset.
    tb = ds_meadow["vaccinations_age"].reset_index()

    #
    # Process data.
    #
    # Harmonize country names
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )

    # Get age group
    age_min = tb.age_group_min.astype(int).astype("string")
    age_max = tb.age_group_max.astype("Int64").apply(lambda x: str(int(x)) if not pd.isna(x) else "+")
    tb["age_group"] = (age_min + "-" + age_max).replace(to_replace=r"-\+", value="+", regex=True)
    tb = tb.drop(columns=["age_group_min", "age_group_max"])

    # Format
    tb = tb.format(["country", "age_group", "date"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[tb],
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
        formats=["feather", "csv"],
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def add_regional_aggregates(tb: Table, ds_regions: Dataset, index_columns: Optional[List[str]] = None) -> Table:
    tb_agg = add_regions(
        tb, ds_regions, keep_only_regions=True, regions={"European Union (27)": {}}, index_columns=index_columns
    )
    tb = concat([tb, tb_agg], ignore_index=True)
    return tb


def fill_time_gaps(tb: Table) -> Table:
    """Interpolate missing time gaps.

    Often, values for certain countries are missing. This can lead to very large under-estimates regional values. To mitigate this, we combine zero-filling with interpolation and other techniques.
    """
    tb = expand_time_column(tb, dimension_col=["country", "vaccine"], time_col="date", method="full_range")
    # cumulative metrics: Interpolate, forward filling (for latest) + zero-filling (for remaining NaNs, likely at start)
    cols_ffill = [
        "total_vaccinations",
    ]
    tb = _interp_ffill_fillna(tb, cols_ffill, ["country", "vaccine"], "date")
    tb[cols_ffill] = tb[cols_ffill].astype(int)
    return cast(Table, tb)


def _interp_ffill_fillna(
    tb: Table, columns: List[str], entity_col: str | List[str] = "country", time_col: str = "date"
) -> Table:
    if isinstance(entity_col, str):
        columns_all = columns + [entity_col, time_col]
    else:
        columns_all = columns + entity_col + [time_col]
    tb = interpolate_table(
        df=tb.loc[:, columns_all],
        entity_col=entity_col,
        time_col=time_col,
        time_mode="none",
    )

    tb.loc[:, columns] = tb.groupby(entity_col)[columns].ffill().fillna(0)  # type: ignore

    return tb
