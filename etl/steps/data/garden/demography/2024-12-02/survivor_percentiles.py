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
from scipy.integrate import cumulative_trapezoid as cumtrapz
from scipy.interpolate import InterpolatedUnivariateSpline

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    paths.log.info("load data.")
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("hmd")

    # Read table from meadow dataset.
    tb_deaths = ds_meadow.read("deaths")
    tb_exposure = ds_meadow.read("exposures")

    #
    # Process data.
    #
    # Combine tables, drop NaNs
    tb = tb_deaths.merge(tb_exposure, on=["country", "year", "sex", "age"], how="outer")
    tb = tb.dropna(subset=["deaths", "exposure"], how="any")

    # Keep format="1x1", and sex="both"
    paths.log.info("keep period & 1-year data.")
    tb = tb.loc[tb["age"].str.match(r"^(\d{1,3}|d{3}\+)$") & (tb["type"] == "period")]

    # Drop unused columns
    tb = tb.drop(columns=["type"])

    # 110+ -> 110
    paths.log.info("replace 110+ -> 100, set Dtypes.")
    tb["age"] = tb["age"].replace({"110+": "110"}).astype(int)

    # Sort
    tb = tb.sort_values(["year", "age"])

    # Actual calculation
    paths.log.info("calculate surviorship ages (can take some minutes)...")
    columns_grouping = ["country", "sex", "year"]
    tb = tb.groupby(columns_grouping).apply(lambda group: obtain_survivorship_ages(group)).reset_index()  # type: ignore

    # Unpivot
    paths.log.info("reshape table")
    tb = tb.melt(
        id_vars=["country", "sex", "year"],
        value_vars=["s1", "s10", "s20", "s30", "s40", "s50", "s60", "s70", "s80", "s90", "s99"],
        var_name="percentile",
        value_name="age",
    )
    tb = tb.dropna(subset=["percentile"])
    tb["percentile"] = tb["percentile"].str.replace("s", "").astype(int)
    tb["percentile"] = 100 - tb["percentile"]

    # Propagate metadata
    tb["age"].metadata.origins = tb_exposure["exposure"].m.origins.copy()

    # Set index
    paths.log.info("format")
    tb = tb.format(["country", "year", "sex", "percentile"], short_name="survivor_percentiles")

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
