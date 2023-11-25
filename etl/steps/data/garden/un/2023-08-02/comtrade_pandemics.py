"""Load a meadow dataset and create a garden dataset.

- Formatting of the table.
- Group some commodities:
    - 'All' category: groups all commodities from this dataset (only concern pandemics).
    = 'All handwear': groups all handwear-related commodities.
"""

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Logger
log = get_logger()
# Assign column name to each CMD code
CMD_CODE_TO_METRIC_NAME = {
    902000: "import_cif_breathing_appliances",
    401511: "import_cif_surgical_gloves",
    392620: "import_cif_plastics_clothing",
    900490: "import_cif_spectacles_goggles",
    630790: "import_cif_textiles_made_up_articles",
    401519: "import_cif_non_surgical_gloves",
    611610: "import_cif_gloves_mittens_mitts_knitted_croacheted",
    621600: "import_cif_gloves_mittens_mitts_not_knitted_croacheted",
}
# The following commodities are grouped into 'All handwear' category
COMODITIES_HANDWEAR = [
    "import_cif_gloves_mittens_mitts_knitted_croacheted",
    "import_cif_gloves_mittens_mitts_not_knitted_croacheted",
    "import_cif_non_surgical_gloves",
]
# 392620   Plastics; articles of apparel and clothing accessories (including gloves)                                                                                             1558
#          Plastics; articles of apparel and clothing accessories (including gloves, mittens and mitts)                                                                          3141
# 401511   Rubber; vulcanised (other than hard rubber), surgical gloves                                                                                                          4510
# 401519   Rubber; vulcanised (other than hard rubber), gloves other than surgical gloves                                                                                        1498
#          Rubber; vulcanised (other than hard rubber), gloves, mittens and mitts other than surgical gloves                                                                     3131
# 611610   Gloves; knitted or crocheted, impregnated, coated or covered with plastics or rubber                                                                                   592
#          Gloves, mittens and mitts; knitted or crocheted, impregnated, coated or covered with plastics or rubber                                                               3789
#          Gloves, mittens and mitts; knitted or crocheted, impregnated, coated, covered or laminated with plastics or rubber                                                      77
# 621600   Gloves, mittens and mitts (not knitted or crocheted)                                                                                                                  4610
# 630790   Textiles; made up articles (including dress patterns), n.e.s. in chapter 63, n.e.s. in heading no. 6307                                                               2335
#          Textiles; made up articles (including dress patterns), n.e.c. in chapter 63, n.e.c. in heading no. 6307                                                               2319
# 900490   Spectacles, goggles and the like; (other than sunglasses) corrective, protective or other                                                                             4672
# 902000   Breathing appliances and gas masks; excluding protective masks having neither mechanical parts nor replaceable filters and excluding apparatus of item no. 9019.20    4627


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("comtrade_pandemics")
    # Load population dataset
    ds_population = paths.load_dataset("population")

    # Load regions dataset.
    ds_regions = paths.load_dataset("regions")

    # Read table from meadow dataset.
    tb = ds_meadow["comtrade_pandemics"].reset_index()

    #
    # Process data.
    #
    # Sanity checks
    paths.log.info("sanity checks")
    _sanity_checks(tb)

    # Keep relevant columns
    log.info("keep relevant columns")
    COLUMNS_RELEVANT = ["refyear", "reporterdesc", "cmdcode", "cifvalue"]
    tb = tb[COLUMNS_RELEVANT]

    # Rename cmd codes for their metric names
    paths.log.info("map cmd codes to indicator names")
    tb["cmdcode"] = tb["cmdcode"].map(CMD_CODE_TO_METRIC_NAME)
    assert (
        not tb["cmdcode"].isna().any()
    ), "Unassigned metric name to some cmd code. Please review consistency between `CMD_CODE_TO_METRIC_NAME` and `cmdcode`."

    # Rename year and country column fields
    paths.log.info("rename columns")
    tb = tb.rename(columns={"refyear": "year", "reporterdesc": "country"})

    # Pivot to get wide format (one column for each cmd item)
    paths.log.info("pivot table")
    tb = tb.pivot(index=["year", "country"], columns="cmdcode", values="cifvalue").reset_index()

    # Check that there is no intersection between former and current countries
    paths.log.info("handle former countries West Germany and Sudan (former)")
    assert (
        tb[tb["country"].str.contains("Germany")].groupby("year").size().max() == 1
    ), "There are some years with data for both Germany and West Germany"
    assert (
        tb[tb["country"].str.contains("Sudan")].groupby("year").size().max() == 1
    ), "There are some years with data for both Sudan and Sudan (former)"
    # West Germany and Sudan (former) are both mapped to current countries (Germany and Sudan).
    # This is to ease the region aggregate estimations.
    # However, later, we undo this for the specific regions. That's why we need the year range.
    YEAR_GERMANY = tb.loc[tb["country"] == "Fed. Rep. of Germany (...1990)", "year"].max()
    YEAR_SUDAN = tb.loc[tb["country"] == "Sudan (...2011)", "year"].max()

    # Harmonize country names
    paths.log.info("harmonize country names")
    tb: Table = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    # Add total handwear
    paths.log.info("add total handwear")
    tb = add_total_handwear(tb)

    # Add total
    paths.log.info("add total")
    tb = add_total(tb)

    # Add regions
    log.info("add regions")
    tb = add_regions(tb, ds_regions)

    # Correct former country names
    paths.log.info("finish handling former countries West Germany and Sudan (former)")
    tb.loc[(tb["country"] == "Germany") & (tb["year"] <= YEAR_GERMANY), "country"] = "West Germany"
    tb.loc[(tb["country"] == "Sudan") & (tb["year"] <= YEAR_SUDAN), "country"] = "Sudan (former)"

    # Add region='World'
    paths.log.info("add region='World'")
    tb = add_world(tb)

    # Add per capita metrics
    paths.log.info("add per capita")
    tb = add_per_capita_variables(tb, ds_population)

    # Set index
    paths.log.info("set index")
    tb = tb.set_index(["year", "country"], verify_integrity=True).sort_index()

    # Set table's short_name
    tb.metadata.short_name = paths.short_name

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def _sanity_checks(tb: Table):
    assert set(tb["typecode"]) == {"C"}, "Type code other than 'C' detected!"
    assert set(tb["freqcode"]) == {"A"}, "Frequency code other than 'A' detected!"
    assert set(tb["flowcode"]) == {"M"}, "Flow code other than 'M' detected!"
    assert set(tb["refmonth"]) == {52}, "Reference month other than '52' detected!"

    assert (tb["period"] == tb["refyear"]).all(), "period != refyear!"
    assert (
        tb.groupby(["refyear", "reporterdesc", "cmdcode"]).size().max() == 1
    ), "There should be, at most, one entry per (refyear, reporterdesc, cmdcode) triplet"


def add_total_handwear(tb: Table) -> Table:
    """Add total aggregate to the table."""
    tb["import_cif_total_handwear"] = tb[COMODITIES_HANDWEAR].sum(axis=1)
    return tb


def add_total(tb: Table) -> Table:
    """Add total aggregate to the table."""
    tb["import_cif_total_pandemics"] = tb[CMD_CODE_TO_METRIC_NAME.values()].sum(axis=1)
    return tb


def add_regions(tb: Table, ds_regions: Dataset) -> Table:
    """Add region aggregates to the table."""
    regions = [
        "Africa",
        "Asia",
        "Europe",
        "North America",
        "South America",
        "Oceania",
    ]
    for region in regions:
        countries_in_region = geo.list_members_of_region(
            region=region,
            ds_regions=ds_regions,
        )
        tb_region = geo.add_region_aggregates(tb, region, countries_in_region=countries_in_region)
        tb = pr.concat(
            [
                tb[tb["country"] != region],
                tb_region[tb_region["country"] == region],
            ],
            ignore_index=True,
        )

        tb = tb.reset_index(drop=True)

    return tb


def add_world(tb: Table) -> Table:
    """Add world aggregate to the table."""
    columns = list(CMD_CODE_TO_METRIC_NAME.values())
    tb_world = tb.groupby("year", as_index=False)[columns].sum()
    tb_world["country"] = "World"

    # Combine
    log.info("combine all tables")
    tb = pr.concat(
        [
            tb,
            tb_world,
        ],
        ignore_index=True,
    )

    return tb


def add_per_capita_variables(tb: Table, ds_population: Dataset) -> Table:
    """Add per-capita variables.

    Parameters
    ----------
    tb : Table
        Primary data.
    ds_population : Dataset
        Population dataset.
    Returns
    -------
    tb : Table
        Data after adding per-capita variables.
    """
    tb = tb.copy()

    # Estimate per-capita variables.
    ## Add population variable
    tb = geo.add_population_to_dataframe(tb, ds_population, expected_countries_without_population=[])
    ## Estimate ratio
    for col in tb.columns:
        if col not in ["population", "year", "country"]:
            tb[f"{col}_per_capita"] = tb[col] / tb["population"]

    # Drop unnecessary column.
    tb = tb.drop(columns=["population"])

    return tb
