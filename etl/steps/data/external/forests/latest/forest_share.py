"""
This step combines multiple historical datasets on forest cover into a single dataset.
The datasets include:
    - China (1000-1960): [He et al. (2025): Reconstructing forest and grassland cover changes in China over the past millennium](https://link.springer.com/article/10.1007/s11430-024-1454-4)
    - Costa Rica (1940-1969): [Kleinn et al. (2002): Forest area in Costa Rica: A comparative study of tropical forest cover estimates over time](https://link.springer.com/article/10.1023/A:1012659129083)
    - England (1086-1650): [Department for Environment, Food and Rural Affairs (DEFRA) report: Government Forestry and Woodlands Policy Statement](https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/221023/pb13871-forestry-policy-statement.pdf)
    - England (1870-1980): [Forest Research report: National inventory of woodland and trees](https://www.forestresearch.gov.uk/tools-and-resources/national-forest-inventory/national-inventory-of-woodland-and-trees/national-inventory-of-woodland-and-trees-england/)
    - France (1000-1976): [Mather et al. (1999): The course and drivers of the forest transition in France](https://www.sciencedirect.com/science/article/abs/pii/S0743016798000230)
    - Japan (1600-1985): [Saito, O. (2009): Forest history and the Great Divergence: China, Japan, and the West compared](https://www.cambridge.org/core/journals/journal-of-global-history/article/abs/forest-history-and-the-great-divergence-china-japan-and-the-west-compared/6140D78077980694B07B40B6396C0343)
    - Philippines (1934-1988): [Center for International Forestry Research (CIFOR) report: One century of forest rehabilitation in the Philippines](https://www.cifor.org/publications/pdf_files/Books/Bchokkalingam0605.pdf&sa=D&source=editors&ust=1747056384183598&usg=AOvVaw1Hqe87fsPmuVQLF1K6hWYO)
    - Scotland (1600-1750): [Mather, A.S. (2008)](https://www.tandfonline.com/doi/pdf/10.1080/00369220418737194)
    - Scotland (1870-1988): [Forest Research report: National inventory of woodland and trees](https://www.forestresearch.gov.uk/tools-and-resources/national-forest-inventory/national-inventory-of-woodland-and-trees/national-inventory-of-woodland-and-trees-scotland/)
    - South Korea (1948-1980): [Bae J.S. et al. (2012) Forest transition in South Korea: Reality, path and drivers](https://www.sciencedirect.com/science/article/pii/S0264837711000615)
    - Taiwan (1904-1982): [Chen Y et al (2019) Reconstructing Taiwan’s land cover changes between 1904 and 2015 from historical maps and satellite images](https://pmc.ncbi.nlm.nih.gov/articles/PMC6403323/)
    - United States (1630-1907): [U.S. Department of Agriculture Forest Service (USDA FS) report: U.S. Forest Facts and Historical Trends](https://web.archive.org/web/20220728061823/https://www.fia.fs.fed.us/library/brochures/docs/2000/ForestFactsMetric.pdf)
    - United States (1920-1987): [U.S. Department of Agriculture Forest Service (USDA FS) report (2014): U.S. Forest Resource Facts and Historical Trends](https://www.fs.usda.gov/sites/default/files/legacy_files/media/types/publication/field_pdf/forestfacts-2014aug-fs1035-508complete.pdf)
    - Vietnam (1943-1985): [Forest Science Institute of Vietnam (FSIV) and Food and Agriculture Organization of the United Nations (FAO) report: Vietname Forestry Outlook Study](https://web.archive.org/web/20230715025310/http://www.fao.org/3/am254e/am254e00.pdf)

"""

from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    # Defra data for England and Scotland
    snap_meadow_defra = paths.load_snapshot("forest_share", namespace="defra")
    # FAO data for England and Scotland
    snap_meadow_fao = paths.load_snapshot("forest_share", namespace="fao")
    # Forest cover data for  Scotland
    snap_meadow_forest_research = paths.load_snapshot("forest_share", namespace="forest_research")
    # Forest cover data for France
    snap_meadow_france = paths.load_snapshot("france_forest_share", namespace="papers")
    # Forest cover data for Japan
    snap_meadow_japan = paths.load_snapshot("japan_forest_share", namespace="papers")
    # Forest cover data for Taiwan
    snap_meadow_taiwan = paths.load_snapshot("taiwan_forest_share", namespace="papers")
    # Forest cover data for the Scotland
    snap_meadow_scotland = paths.load_snapshot("mather_2004", namespace="papers")
    # Forest cover data for Costa Rica
    snap_meadow_costa_rica = paths.load_snapshot("kleinn_2000", namespace="papers")
    # Forest research data for South Korea
    snap_meadow_south_korea = paths.load_snapshot("soo_bae_et_al_2012", namespace="papers")
    # Forest research data for USA
    snap_meadow_usa = paths.load_snapshot("forest_share", namespace="usda_fs")
    # Forest data for China
    snap_meadow_china = paths.load_snapshot("he_2025", namespace="papers")
    # More recent forest data for England and Scotland - from the Scottish Government
    snap_meadow_sg = paths.load_snapshot("scottish_government", namespace="papers")

    # Read table from meadow dataset.
    tb_defra = snap_meadow_defra.read()
    tb_fao = snap_meadow_fao.read()
    tb_forest_research = snap_meadow_forest_research.read()
    tb_france = snap_meadow_france.read()
    tb_japan = snap_meadow_japan.read()
    tb_taiwan = snap_meadow_taiwan.read()
    tb_scotland = snap_meadow_scotland.read()
    tb_costa_rica = snap_meadow_costa_rica.read()
    tb_south_korea = snap_meadow_south_korea.read()
    tb_usa = snap_meadow_usa.read()
    tb_china = snap_meadow_china.read()
    tb_sg = snap_meadow_sg.read()
    # Concatenate tables.
    tb = pr.concat(
        [
            tb_defra,
            tb_fao,
            tb_forest_research,
            tb_france,
            tb_japan,
            tb_taiwan,
            tb_scotland,
            tb_costa_rica,
            tb_south_korea,
            tb_usa,
            tb_china,
            tb_sg,
        ]
    )
    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=snap_meadow_defra.metadata, formats=["csv"])
    # Save garden dataset.

    ds_garden.save()
