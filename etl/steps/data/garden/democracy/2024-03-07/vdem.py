"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("vdem")

    # Read table from meadow dataset.
    tb = ds_meadow["vdem"].reset_index()

    tb = tb.astype(
        {
            "v2exnamhos": str,
        }
    )
    #
    # Process data.
    #
    # Drop superfluous observations
    tb = tb[~((tb["country"] == "Italy") & (tb["year"] == 1861))]
    tb.loc[(tb["country"] == "Piedmont-Sardinia") & (tb["year"] == 1861), "country"] = "Italy"

    # Goemans et al.'s (2009) Archigos dataset, rulers.org, and worldstatesmen.org identify non-elected General Raoul Cédras as the de-facto leader of Haiti from 1991 until 1994.
    tb.loc[(tb["country"] == "Haiti") & (tb["year"] >= 1991) & (tb["year"] <= 1993), "v2exnamhos"] = "Raoul Cédras"
    tb.loc[(tb["country"] == "Haiti") & (tb["year"] >= 1991) & (tb["year"] <= 1993), "v2ex_hosw"] = 1
    tb.loc[(tb["country"] == "Haiti") & (tb["year"] >= 1991) & (tb["year"] <= 1993), "v2ex_hogw"] = 0
    tb.loc[(tb["country"] == "Haiti") & (tb["year"] >= 1991) & (tb["year"] <= 1993), "v2exaphogp"] = 0

    # While the head-of-government indicators generally should refer to the one in office on December 31, v2exfemhog seems to (occasionally?) refer to other points during the year. For most purposes, it makes sense to consistently refer to December 31, so I am recoding here.
    ## HOG
    tb.loc[tb["v2exnamhog"] == "Diango Cissoko", "v2exfemhog"] = 0
    tb.loc[tb["v2exnamhog"] == "Ion Chicu", "v2exfemhog"] = 0
    tb.loc[tb["v2exnamhog"] == "Joseph Jacques Jean Chrétien", "v2exfemhog"] = 0
    tb.loc[tb["v2exnamhog"] == "KåreIsaachsen Willoch", "v2exfemhog"] = 0
    tb.loc[tb["v2exnamhog"] == "YuriiIvanovych Yekhanurov", "v2exfemhog"] = 0
    ## HOS
    tb.loc[tb["v2exnamhos"] == "Ali Ben Bongo Ondimba", "v2exfemhos"] = 0
    tb.loc[tb["v2exnamhos"] == "Chulalongkorn (Rama V)", "v2exfemhos"] = 0
    tb.loc[tb["v2exnamhos"] == "Dieudonné François Joseph Marie Reste", "v2exfemhos"] = 0
    tb.loc[tb["v2exnamhos"] == "Letsie III", "v2exfemhos"] = 0
    tb.loc[tb["v2exnamhos"] == 'Miguel I "o Rei Absoluto"', "v2exfemhos"] = 0
    tb.loc[tb["v2exnamhos"] == "Moshoeshoe II", "v2exfemhos"] = 0
    tb.loc[tb["v2exnamhos"] == "Prince Zaifeng", "v2exfemhos"] = 0
    tb.loc[tb["v2exnamhos"] == "Rajkeswur Purryag", "v2exfemhos"] = 0
    tb.loc[tb["v2exnamhos"] == "Shimon Peres", "v2exfemhos"] = 0

    # Sort
    tb = tb.sort_values(["country", "year"])

    # Something
    columns = [
        "v2elmulpar_osp",
        "v2elmulpar_osp_codehigh",
        "v2elmulpar_osp_codelow",
        "v2elfrfair_osp",
    ]
    mask = tb["v2x_elecreg"] == 1
    for col in columns:
        ffilled = tb[col].ffill()
        mask = tb["v2x_elecreg"] == 1
        tb.loc[mask, col] = ffilled.loc[mask]

    # tb["year_diff"] = tb.groupby("country").year.diff()
    # column_name = "v2elmulpar_osp"
    # tb.loc[(tb["diff"]!=1) & (tb[column_name].isna()) & (tb["v2x_elecreg"].isna())].empty

    # For v2elmulpar_osp_imp
    tb["v2elmulpar_osp_imp"] = tb["v2elmulpar_osp"]
    tb.loc[(tb["v2elmulpar_osp_imp"].isna()) & (tb["v2x_elecreg"] == 1), "v2elmulpar_osp_imp"] = tb[
        "v2elmulpar_osp_imp"
    ].shift(1)

    # For v2elmulpar_osp_high_imp
    tb["v2elmulpar_osp_high_imp"] = tb["v2elmulpar_osp_codehigh"]
    tb.loc[(tb["v2elmulpar_osp_high_imp"].isna()) & (tb["v2x_elecreg"] == 1), "v2elmulpar_osp_high_imp"] = tb[
        "v2elmulpar_osp_high_imp"
    ].shift(1)

    # For v2elmulpar_osp_low_imp
    tb["v2elmulpar_osp_low_imp"] = tb["v2elmulpar_osp_codelow"]
    tb.loc[(tb["v2elmulpar_osp_low_imp"].isna()) & (tb["v2x_elecreg"] == 1), "v2elmulpar_osp_low_imp"] = tb[
        "v2elmulpar_osp_low_imp"
    ].shift(1)

    # Harmonize country names
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    tb = tb.set_index(["country", "year"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
