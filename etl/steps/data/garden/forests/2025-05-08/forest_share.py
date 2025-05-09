"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    # Defra data for England and Scotland.
    ds_meadow_defra = paths.load_snapshot("forest_share", namespace="defra")
    # FAO data for England and Scotland.
    ds_meadow_fao = paths.load_snapshot("forest_share", namespace="fao")
    # Forest cover data for  Scotland.
    ds_meadow_forest_research = paths.load_snapshot("forest_share", namespace="forest_research")
    # Forest cover data for France
    ds_meadow_france = paths.load_snapshot("france_forest_share", namespace="papers")
    # Forest cover data for Japan.
    ds_meadow_japan = paths.load_snapshot("japan_forest_share", namespace="papers")
    # Forest cover data for Taiwan.
    ds_meadow_taiwan = paths.load_snapshot("taiwan_forest_share", namespace="papers")
    # Forest cover data for the Scotland
    ds_meadow_scotland = paths.load_snapshot("mather_2004", namespace="papers")
    # Forest research data for South Korea
    ds_meadow_south_korea = paths.load_snapshot("soo_bae_et_al_2012", namespace="papers")
    # Forest research data for USA
    ds_meadow_usa = paths.load_snapshot("forest_share", namespace="usda_fs")
    # FAO RL
    ds_meadow_fao_rl = paths.load_dataset("faostat_rl")

    # Read table from meadow dataset.
    tb_defra = ds_meadow_defra.read("forest_share")
    tb_fao = ds_meadow_fao.read("forest_share")
    tb_forest_research = ds_meadow_forest_research.read("forest_share")
    tb_france = ds_meadow_france.read("france_forest_share")
    tb_japan = ds_meadow_japan.read("japan_forest_share")
    tb_taiwan = ds_meadow_taiwan.read("taiwan_forest_share")
    tb_scotland = ds_meadow_scotland.read("mather_2004")
    tb_south_korea = ds_meadow_south_korea.read("soo_bae_et_al_2012")
    tb_usa = ds_meadow_usa.read("forest_share")

    # Concatenate tables.
    tb = pr.concat(
        [tb_defra, tb_fao, tb_forest_research, tb_france, tb_japan, tb_taiwan, tb_scotland, tb_south_korea, tb_usa]
    )

    #
    # Process data.
    #
    # Harmonize country names.
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
