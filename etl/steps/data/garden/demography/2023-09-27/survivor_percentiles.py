"""Load a meadow dataset and create a garden dataset.

Methods used here are taken from https://github.com/jssalvrz/s-ages. Authors of Citation: Alvarez, J.-A., & Vaupel, J. W. (2023). Mortality as a Function of Survival. Demography, 60(1), 327–342. https://doi.org/10.1215/00703370-10429097


Dr. Saloni Dattani translated the R scripts into Python:
    - Original: https://github.com/jssalvrz/s-ages
    - Translated: https://github.com/saloni-nd/misc/tree/main/survivorship-ages

Lucas Rodes-Guirao adapted the python code for ETL.
"""

import numpy as np
import pandas as pd
from owid.catalog import Table
from scipy.integrate import cumtrapz
from scipy.interpolate import InterpolatedUnivariateSpline

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    paths.log.info("survivor_percentiles: load data.")
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("hmd")

    # Read table from meadow dataset.
    tb = ds_meadow["hmd"]

    #
    # Process data.
    #
    # Keep relevant columns, drop NaNs and reset index
    paths.log.info("survivor_percentiles: keep relevant columns, drop NaNs, reset index.")
    tb = tb[["deaths", "exposure"]].dropna().reset_index()

    # Get origins
    origins = tb["deaths"].m.origins

    # Keep format="1x1", and sex="both"
    paths.log.info("survivor_percentiles: Use period and 1x1 data.")
    tb = tb[(tb["type"] == "period") & (tb["format"] == "1x1")]
    # Drop unused columns
    tb = tb.drop(columns=["format", "type"])

    # 110+ -> 110
    paths.log.info("survivor_percentiles: replace 110+ -> 100, set Dtypes.")
    tb["age"] = tb["age"].replace({"110+": "110"})

    # Dtypes
    tb = tb.astype(
        {
            "year": "Int64",
            "age": "Int64",
            "deaths": float,
            "exposure": float,
        }
    )

    # Sort
    tb = tb.sort_values(["year", "age"])

    # Debugging (pick just one country to speed up calculations)
    # tb = tb[tb.country == "Denmark"]

    # Actual calculation
    paths.log.info("survivor_percentiles: calculate surviorship ages (can take some minutes)...")
    columns_grouping = ["country", "sex", "year"]
    tb = tb.groupby(columns_grouping).apply(lambda group: obtain_survivorship_ages(group)).reset_index()

    # Unpivot
    paths.log.info("survivor_percentiles: unpivot")
    tb = tb.melt(
        id_vars=["country", "sex", "year"],
        value_vars=["s1", "s10", "s20", "s30", "s40", "s50", "s60", "s70", "s80", "s90", "s99"],
        var_name="percentile",
        value_name="age",
    )
    tb = tb.dropna(subset=["percentile"])
    paths.log.info("survivor_percentiles: rename percentiles (remove 's' and reverse order))")
    tb["percentile"] = tb["percentile"].str.replace("s", "").astype(int)
    tb["percentile"] = 100 - tb["percentile"]

    # Dtypes
    paths.log.info("survivor_percentiles: final dtypes settings.")
    tb = tb.astype(
        {
            "year": int,
            "percentile": int,
        }
    )

    # Set index
    paths.log.info("survivor_percentiles: set index.")
    tb = tb.set_index(["country", "year", "sex", "percentile"], verify_integrity=True)

    # Update metadata
    paths.log.info("survivor_percentiles: metadata.")
    tb.metadata.short_name = paths.short_name
    # Propagate origins metadata (unsure why this has not been propagated?)
    for col in tb.columns:
        tb[col].metadata.origins = origins

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def obtain_survivorship_ages(tb_group: Table, start_age: int = 0, end_age: int = 110) -> pd.DataFrame:
    """Get survivorship ages given a life and deaths table.

    Output dataframe has a column for each percentile of survivorship age.

    tb_group is expected to be a subset of the compelte table. It should only concern a particular (country, year, sex) triple.
    """
    # Step 1: Apply splines, get Mx for each (country, year, sex, age)
    ## Define splines
    ### We could use CubicSpline (k=3 order), but it provides slightly different results hence, for precaution, we sticked to InterpolatedUnivariateSpline.
    ### This is equivalent to R function interpSpline
    spline_deaths = InterpolatedUnivariateSpline(tb_group["age"], tb_group["deaths"], k=3)
    spline_exposures = InterpolatedUnivariateSpline(tb_group["age"], tb_group["exposure"], k=3)

    ## Define age range (with step 0.01)
    age_range = np.arange(start_age, end_age, 0.01)

    # Run splines over age range
    deaths_spline = np.abs(spline_deaths(age_range))
    exposure_spline = np.abs(spline_exposures(age_range))
    exposure_spline[exposure_spline == 0] = np.nan
    survival_age_spline = np.abs(deaths_spline / exposure_spline)

    # Step 2: Calculate survival, density, hazard, and cumulative hazards
    ## Estimate parameters
    Hx = cumtrapz(y=survival_age_spline, x=age_range, initial=0)  # Hazard CDF
    Sx = np.exp(-Hx)  # Survivor function

    # Step 3: Calculate survivorship ages from parameters
    out = {}
    out["s0"] = max(age_range)
    ## I'm using a for loop to simplify the logic here
    for i in range(1, 101):
        try:
            sx_rounded = np.ceil((100 * Sx).round(3))
            value = age_range[sx_rounded == i][0]
            out[f"s{i}"] = value
        except IndexError:
            out[f"s{i}"] = np.nan

    # Create output dataframe
    df = pd.DataFrame(out, index=[0])

    return df
