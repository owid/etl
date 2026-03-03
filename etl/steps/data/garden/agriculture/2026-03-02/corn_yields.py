"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def fix_spurious_zeros(tb):
    """Remove spurious zeros in the data.

    As mentioned in the meadow step, the download tool creates spurious zeros when downloading data for multiple countries. For example, if you go to:
    https://apps.fas.usda.gov/psdonline/app/index.html#/app/advQuery
    And select corn yields for Croatia, it only has data between 1992 and 1998. But if you fetch data from both Croatia and any other country with more years of data informed, e.g. Egypt, the resulting series for Croatia is filled with zeros from 1960 onwards.
    DEBUG: Uncomment to inspect all countries that have at least one zero.
    for country in sorted(set(tb["country"])):
        if not tb[(tb["country"]==country) & (tb["yield"] == 0)].empty:
            px.line(tb[tb["country"]==country], x="year", y="yield", title=country).show()
    It seems safe to assume that most zeros in the data are spurious, and should therefore be removed.
    """
    tb = tb[(tb["yield"] > 0)].reset_index(drop=True)

    return tb


def convert_marketing_year_to_harvesting_year(tb):
    """Adapt USDA data (reported by marketing year) to follow a similar reporting criterion as FAO data (reported by harvesting year).

    According to FAOSTAT:
    https://files-faostat.fao.org/production/QCL/QCL_methodology_e.pdf
    "[...] the data for any particular crop are reported under the calendar year in which the entire harvest or the bulk of it took place".
    In other words, FAOSTAT reports by harvesting year.

    Corn is planted in spring and harvested (and marketed) in autumn. For countries in the Northern Hemisphere, both events occur within the same calendar year. In the Southern Hemisphere, harvesting happens in the following calendar year.

    USDA, on the other hand, reports by marketing year, which spans two calendar years (e.g. 2020/2021, 2021/2022, ...). The harvesting starts on the first year in the pair (for northern countries) and ends on the second year in the pair (for southern countries).

    In the meadow step of the current dataset, I've (arbitrarily) chosen to use the first year in each pair. But, if we want to align FAO with USDA so that both refer to the same crop, we need to adjust countries in the Southern Hemisphere. We can either shift FAO one year backward, or shift USDA data one year forward. For simplicity (given that we have much more data from FAO) we shift USDA data for Southern Hemisphere countries one year forward.

    For more details, see https://github.com/owid/owid-issues/issues/2158#issuecomment-3416318240
    """
    tb = tb.copy()

    # Get list of countries in the Southern Hemisphere.
    countries_south = paths.regions.get_region("Southern Hemisphere")["members"]

    # Shift them one year forward.
    tb.loc[tb["country"].isin(countries_south), "year"] += 1

    # To avoid unnecessary confusion, remove data that appears now "in the future".
    # This is data for the current year, which is uncertain or incomplete in any case.
    current_year = int(tb["yield"].metadata.origins[0].date_published.split("-")[0])
    tb = tb[tb["year"] <= current_year].reset_index(drop=True)

    return tb


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("corn_yields")

    # Read table from meadow dataset.
    tb = ds_meadow.read("corn_yields")

    #
    # Process data.
    #
    # Sanity check.
    assert (tb["unit_description"] == "(MT/HA)").all(), "Unexpected units."
    assert (tb["commodity"] == "Corn").all(), "Unexpected commodity."
    assert (tb["attribute"] == "Yield").all(), "Unexpected attribute."

    # Remove unnecessary columns.
    tb = tb.drop(columns=["unit_description", "commodity", "attribute"])

    # Select relevant columns.
    tb = tb.rename(
        columns={column: column.split("_")[1] for column in tb.columns if column.startswith("_")}, errors="raise"
    )

    # Transpose table.
    tb = tb.melt(id_vars=["country"], var_name="year", value_name="yield").astype({"year": int})

    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb=tb)

    # Fix spurious zeros in the data.
    tb = fix_spurious_zeros(tb=tb)

    # Adapt USDA "marketing years" to become "harvesting years" (as reported by FAO).
    # This, for corn data in this dataset, simply implies shifting the data of Southern Hemisphere countries one year forward.
    tb = convert_marketing_year_to_harvesting_year(tb=tb)

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
