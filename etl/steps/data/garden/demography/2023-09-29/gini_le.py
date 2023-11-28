"""Estimate the gini index on life expectency"""

from typing import Any, cast

import numpy as np
from numpy.typing import NDArray
from owid.catalog import Table, Variable
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Logger
log = get_logger()


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    log.info("gini_le: load data")
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("hmd")

    # Read table from meadow dataset.
    tb = ds_meadow["hmd"].reset_index()

    #
    # Process data.
    #
    # Keep relevant dimensions
    log.info("gini_le: keep relevant dimensions (format='1x1', type='period', sex in ['male', 'female'])")
    tb = tb[(tb["format"] == "1x1") & (tb["type"] == "period") & (tb["sex"].isin(["male", "female"]))]
    log.info("gini_le: set year dtype to int")
    tb["year"] = tb["year"].astype("Int64")

    # Get origins
    origins = tb["life_expectancy"].m.origins

    # Get rate for central_death_rate, as it is given per 1,000 people.
    log.info("gini_le: get rate for central_death_rate, as it is given per 1,000 people.")
    tb["central_death_rate"] = tb["central_death_rate"] / 1000

    # 110+ -> 110
    log.info("gini_le: replace 110+ -> 100, set Dtypes.")
    tb["age"] = tb["age"].replace({"110+": "110"}).astype("Int64")

    # Sort rows
    log.info("gini_le: sort rows (needed for correct estimation)")
    tb = tb.sort_values(["country", "year", "sex", "age"])

    # Estimates
    tb = tb.groupby(["country", "year", "sex"], as_index=False).apply(gini_from_mx)

    # Rename columns
    log.info("gini_le: rename columns")
    tb = tb.rename(columns={"central_death_rate": "life_expectancy_gini"})

    # Set index
    log.info("gini_le: set index")
    tb = tb.set_index(["country", "year", "sex"], verify_integrity=True)

    # Update metadata
    log.info("gini_le: metadata")
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


def AKm02a0(m0: float, is_male: bool = True) -> NDArray[Any]:
    """Get estimates.

    Calculate the average number of years lived in the first year of life (ax for age 0), and is calculated based on the mortality rate during the first year of life (m0).

    There is a slight different procedure for male or female.

    More details: https://www.rdocumentation.org/packages/MortHump/versions/0.2/topics/AKm02a0
    """
    if is_male:
        return np.where(m0 < 0.0230, 0.14929 - 1.99545 * m0, np.where(m0 < 0.08307, 0.02832 + 3.26201 * m0, 0.29915))
    else:
        return np.where(m0 < 0.01724, 0.14903 - 2.05527 * m0, np.where(m0 < 0.06891, 0.04667 + 3.88089 * m0, 0.31411))


def gini_from_mx(tb_group: Table) -> Variable:
    """Get Gini coefficient from central death rate.

    This code is adapted from the original R code: https://github.com/jmaburto/Dynamics_Code/tree/V1.0/R%20code
    """
    # Get values from input
    mx = tb_group["central_death_rate"].values
    is_male = tb_group.name[2] == "male"

    # Estimate i_openage, ax
    i_openage = len(mx)
    m0 = cast(float, mx[0])
    ax = np.full_like(mx, 0.5)
    ax[0] = AKm02a0(m0=m0, is_male=is_male)
    ax[i_openage - 1] = 1 / mx[i_openage - 1]  # type: ignore

    # Estimate X_
    age = np.arange(i_openage) + ax
    e = np.ones_like(age)
    X_ = np.abs(np.outer(e, age) - np.outer(age, e))

    # Estimate D
    OPENAGE = i_openage - 1
    ## Calculates the probability of dying in each age interval
    qx = mx / (1 + (1 - ax) * mx)  # type: ignore
    qx[i_openage - 1] = 1 if not np.isnan(qx[i_openage - 1]) else np.nan
    ## Probability of surviving in each age interval
    px = 1 - qx
    px[np.isnan(px)] = 0
    ## number of survivors at the start of each interval
    RADIX = 1  # starting value
    lx = np.concatenate(([RADIX], RADIX * np.cumprod(px[:OPENAGE])))
    ## number of people who die in each interval
    dx = lx * qx
    ## number of person years lived in each interval
    ## [number of initial survivors in that interval] -  (1 - [number of years lived during that interval]) * [number who die in the interval]
    Lx = lx - (1 - ax) * dx
    Lx[i_openage - 1] = lx[i_openage - 1] * ax[i_openage - 1]
    ## total number of life years from a given age to the end of the cohort
    Tx = np.concatenate((np.cumsum(Lx[:OPENAGE][::-1])[::-1], [0])) + Lx[i_openage - 1]
    ## life expectancy
    ex = Tx / lx
    ## matrix with the number of deaths for each age-pair combination
    D = np.outer(dx, dx)

    # Estimate Gini
    ## total inequality in lifespans: sum of the product of the matrix D by the age difference, np.sum(D * X_)
    ## divided by the life expectancy at birth x2 (helps to normalise it to a number between 0 and 1)
    G = np.sum(D * X_) / (2 * ex[0])

    var = Variable({"life_expectancy_gini": G})
    return var
