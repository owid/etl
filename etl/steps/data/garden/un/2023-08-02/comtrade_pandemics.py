"""Load a meadow dataset and create a garden dataset."""

from typing import cast

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
    392620: "import_cif_plastics_clothing",
    401511: "import_cif_surgical_gloves",
    401519: "import_cif_non_surgical_gloves",
    611610: "import_cif_gloves_mittens_mitts_knitted_croacheted",
    621600: "import_cif_gloves_mittens_mitts_not_knitted_croacheted",
    630790: "import_cif_textiles_made_up_articles",
    900490: "import_cif_spectacles_goggles",
    902000: "import_cif_breathing_appliances",
}

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
    ds_meadow = cast(Dataset, paths.load_dependency("comtrade_pandemics"))

    # Load regions dataset.
    ds_regions: Dataset = paths.load_dependency("regions")

    # Read table from meadow dataset.
    tb = ds_meadow["comtrade_pandemics"].reset_index()

    #
    # Process data.
    #
    # Sanity checks
    log.info("un.comtrade: sanity checks")
    _sanity_checks(tb)

    # Keep relevant columns
    log.info("un.comtrade: keep relevant columns")
    COLUMNS_RELEVANT = ["refyear", "reporterdesc", "cmdcode", "cifvalue"]
    tb = tb[COLUMNS_RELEVANT]

    # Rename cmd codes for their metric names
    log.info("un.comtrade: map cmd codes to indicator names")
    tb["cmdcode"] = tb["cmdcode"].map(CMD_CODE_TO_METRIC_NAME)
    assert (
        not tb["cmdcode"].isna().any()
    ), "Unassigned metric name to some cmd code. Please review consistency between `CMD_CODE_TO_METRIC_NAME` and `cmdcode`."

    # Rename year and country column fields
    log.info("un.comtrade: rename columns")
    tb = tb.rename(columns={"refyear": "year", "reporterdesc": "country"})

    # Pivot to get wide format (one column for each cmd item)
    log.info("un.comtrade: pivot table")
    tb = tb.pivot(index=["year", "country"], columns="cmdcode", values="cifvalue").reset_index()

    # Check that there is no intersection between former and current countries
    log.info("un.comtrade: handle former countries West Germany and Sudan (former)")
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
    log.info("un.comtrade: harmonize country names")
    tb: Table = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    # Add regions
    log.info("un.comtrade: add regions")
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
        tb = pd.concat(
            [
                tb[tb["country"] != region],
                tb_region[tb_region["country"] == region],
            ],
            ignore_index=True,
        ).reset_index(drop=True)

    # Correct former country names
    log.info("un.comtrade: finish handling former countries West Germany and Sudan (former)")
    tb.loc[(tb["country"] == "Germany") & (tb["year"] <= YEAR_GERMANY), "country"] = "West Germany"
    tb.loc[(tb["country"] == "Sudan") & (tb["year"] <= YEAR_SUDAN), "country"] = "Sudan (former)"

    # Add region='World'
    log.info("un.comtrade: add region='World'")
    columns = list(CMD_CODE_TO_METRIC_NAME.values())
    tb_world = tb.groupby("year", as_index=False)[columns].sum()
    tb_world["country"] = "World"

    # Combine
    log.info("un.comtrade: combine all tables")
    tb = pd.concat(
        [
            tb,
            tb_world,
        ],
        ignore_index=True,
    )

    # Set index
    log.info("un.comtrade: set index")
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
    ), "There should, at most, one entry per (refyear, reporterdesc, cmdcode) triplet"
