"""Load a meadow dataset and create a garden dataset.

NOTE on unit conversions:
Broadberry et al. (2015) has an appendix on "Weights, measures and money", where they define "Imperial measures and
their metric equivalents". They define 1 bushel of dry volume as 35.238 litres. According to the Britannica dictionary
(https://www.britannica.com/science/bushel) this corresponds, not to the current Imperial bushel, but to the US (or
Winchester) bushel. However, Britannica also explains that the Winchester bushel used to be used in England from the
15th century until 1824. I suppose that is the reason why Broadberry et al. (2015) uses it.
This corresponds to a conversion of 1 bushel = 0.02819 litres, which is (within less than 1%) in agreement with the
definition from the USDA (https://www.ers.usda.gov/webdocs/publications/41880/33132_ah697_002.pdf), according to which:
1 litre of dry measure = 0.02837759 bushels
Therefore, the definition of bushel from Broadberry et al. (2015) seems to agree with that of the USDA, and therefore we
can directly use the conversions given by the USDA, that are as follows:
* 60-pound bushel of wheat, white potatoes, and soybeans: 1 bushel = 0.0272155 metric tons.
* 56-pound bushel of shelled corn, rye, sorghum grain, and flaxseed: 1 bushel = 0.0254 metric tons.
* 48-pound bushel of barley, buckwheat, and apples: 1 bushel = 0.021772 metric tons.
* 32-pound bushel of oats: 1 bushel = 0.014515 metric tons.
USDA also defines "38-pound bushel of oats: 1 bushel = 0.01724 metric tons". But it seems the 32-pound per bushel of oat
is more common than the 38-pound one (and in Table 6 of that same USDA document they use 32 pounds for oats).
They don't have an equivalence of bushels to metric tons for pulses, but I assume the same value as wheat and soybeans
(which in Table 6 is also the one used for, e.g., green peas and lentils).

"""

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Unit conversion factors to change from bushels to metric tonnes.
BUSHELS_TO_TONNES = {
    "wheat": 0.0272155,
    "potatoes": 0.0272155,
    "pulses": 0.0272155,
    "rye": 0.0254,
    "barley": 0.021772,
    "oats": 0.014515,
}

# Unit conversion factor to change from acres to hectares.
ACRES_TO_HECTARES = 0.4047


def run(dest_dir: str) -> None:
    log.info("broadberry_et_al_2015.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency("broadberry_et_al_2015")

    # Read table from meadow dataset.
    tb_meadow = ds_meadow["broadberry_et_al_2015"]

    # Create a dataframe with data from the table.
    df = pd.DataFrame(tb_meadow)

    #
    # Process data.
    #
    # Data is given as decadal averages. Use the average year of each decade (e.g. instead of 1300, use 1305).
    df = df.rename(columns={"decade": "year"})
    df["year"] += 5

    # Add a country column, set an appropriate index and sort conveniently
    df = (
        df.rename(columns={"decade": "year"})
        .assign(**{"country": "United Kingdom"})
        .set_index(["country", "year"], verify_integrity=True)
        .sort_index()
    )

    # Rename columns.
    df.columns = [f"{column}_yield" for column in df.columns]

    # Ensure all numeric columns are standard floats, and convert units (from bushels per acre to tonnes per hectare).
    for column in df.columns:
        commodity = column.replace("_yield", "")
        df[column] = df[column].astype(float) * BUSHELS_TO_TONNES[commodity] / ACRES_TO_HECTARES

    # Create a new table with the processed data.
    tb_garden = Table(df, short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("broadberry_et_al_2015.end")
