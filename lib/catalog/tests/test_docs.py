"""Tests for the documentation rendered from metadata: codebook, sources, README, Excel workbook."""

import copy
from pathlib import Path

from owid.catalog import Dataset, DatasetMeta, License, Origin, Table
from owid.catalog.core import docs

EI = Origin(
    producer="Energy Institute",
    title="Statistical Review of World Energy",
    date_published="2026-06-30",
    date_accessed="2026-07-02",
    url_main="https://www.energyinst.org/statistical-review/",
    citation_full="Energy Institute - Statistical Review of World Energy (2026).",
    license=License(name="© Energy Institute 2026", url="https://www.energyinst.org/terms"),
)
EI_COPY = copy.deepcopy(EI)
POPULATION = Origin(
    producer="Various sources",
    title="Population",
    attribution="Population based on various sources (2024)",
    date_published="2024-07-15",
    url_main="https://ourworldindata.org/population-sources",
    license=License(name="CC BY 4.0", url="https://creativecommons.org/licenses/by/4.0/"),
)


def make_table() -> Table:
    tb = Table(
        {
            "country": ["Spain", "Spain", "France"],
            "year": [2020, 2021, 2020],
            "hydro_energy_twh": [1.0, 2.0, 3.0],
            "hydro_energy_per_capita_kwh": [10.0, None, 30.0],
        },
        short_name="energy",
    )
    tb["hydro_energy_twh"].metadata.title = "Hydropower"
    tb["hydro_energy_twh"].metadata.description_short = "Energy from [hydropower](#dod:hydro)."
    tb["hydro_energy_twh"].metadata.unit = "terawatt-hours"
    tb["hydro_energy_twh"].metadata.short_unit = "TWh"
    tb["hydro_energy_twh"].metadata.origins = [EI]
    tb["hydro_energy_twh"].metadata.description_from_producer = "Hydroelectric output, gross."
    tb["hydro_energy_twh"].metadata.description_processing = "Converted from exajoules."
    tb["hydro_energy_per_capita_kwh"].metadata.title = "Hydropower per capita"
    tb["hydro_energy_per_capita_kwh"].metadata.unit = "kilowatt-hours per person"
    tb["hydro_energy_per_capita_kwh"].metadata.origins = [EI_COPY, POPULATION]
    tb["country"].metadata.title = "Country"
    tb["year"].metadata.title = "Year"
    return tb.set_index(["country", "year"])


def make_dataset(path: Path) -> Dataset:
    ds = Dataset.create_empty(
        path,
        DatasetMeta(
            channel="garden",
            namespace="energy",
            short_name="owid_energy",
            version="2026-09-10",
            title="Energy dataset",
            description="Key energy metrics.",
            changelog=[
                {"date": "2025-07-17", "changes": ["Updated sources.\n- Including the Energy Institute."]},
                {"date": "2026-09-10", "changes": ["New methodology.", "Renamed columns."]},
            ],
            licenses=[License(name="CC BY 4.0", url="https://creativecommons.org/licenses/by/4.0/")],
        ),
    )
    ds.add(make_table())
    ds.save()
    return ds


def test_origin_label_defaults_to_producer_title_year():
    assert docs.origin_label(EI) == "Energy Institute – Statistical Review of World Energy (2026)"


def test_origin_label_prefers_attribution():
    assert docs.origin_label(POPULATION) == "Population based on various sources (2024)"


def test_origin_label_without_title_or_date():
    assert docs.origin_label(Origin(producer="Smil", title="Smil", date_published="latest")) == "Smil"


def test_codebook_uses_short_source_labels():
    codebook = make_table().codebook
    assert codebook.columns.tolist() == [
        "column",
        "title",
        "description",
        "unit",
        "date_range",
        "source",
        "description_key",
        "description_from_producer",
        "description_processing",
    ]
    assert codebook["column"].tolist() == ["country", "year", "hydro_energy_twh", "hydro_energy_per_capita_kwh"]
    hydro = codebook.set_index("column").loc["hydro_energy_twh"]
    assert hydro["title"] == "Hydropower"
    assert hydro["description"] == "Energy from hydropower."
    assert hydro["unit"] == "terawatt-hours (TWh)"
    assert hydro["date_range"] == "2020–2021"
    assert codebook.set_index("column").loc["hydro_energy_per_capita_kwh", "date_range"] == "2020–2020"
    assert hydro["source"] == "Energy Institute – Statistical Review of World Energy (2026)"
    assert hydro["description_from_producer"] == "Hydroelectric output, gross."
    assert hydro["description_processing"] == "Converted from exajoules."
    assert hydro["description_key"] == ""
    per_capita = codebook.set_index("column").loc["hydro_energy_per_capita_kwh"]
    assert per_capita["source"] == (
        "Energy Institute – Statistical Review of World Energy (2026); Population based on various sources (2024)"
    )


def test_sources_deduplicates_and_orders_by_usage():
    sources = make_table().sources
    assert sources.columns.tolist() == docs.SOURCES_COLUMNS
    # The Energy Institute origin appears in two columns (as two equal objects); population in one.
    assert sources["label"].tolist() == [
        "Energy Institute – Statistical Review of World Energy (2026)",
        "Population based on various sources (2024)",
    ]
    ei = sources.set_index("label").loc["Energy Institute – Statistical Review of World Energy (2026)"]
    assert ei["url_main"] == "https://www.energyinst.org/statistical-review/"
    assert ei["date_accessed"] == "2026-07-02"


def test_codebook_labels_match_sources_labels():
    tb = make_table()
    labels = {label for cell in tb.codebook["source"] for label in cell.split("; ") if label}
    assert labels == set(tb.sources["label"])


def test_readme_sections(tmp_path):
    ds = make_dataset(tmp_path / "owid_energy")
    readme = ds.readme(url="https://catalog.ourworldindata.org/energy/owid_energy/")
    assert readme.startswith("# Energy dataset\n\n## About this dataset\n\nKey energy metrics.\n")
    assert "This file documents" not in readme
    # The changelog is its own section, after the data and newest release first; not part of the description.
    assert "## About this dataset\n\nKey energy metrics.\n\n## Data\n" in readme
    assert (
        "## Changelog\n\n"
        "- 2026-09-10:\n  - New methodology.\n  - Renamed columns.\n"
        "- 2025-07-17:\n  - Updated sources.\n    - Including the Energy Institute.\n\n"
        "## How we process data\n"
    ) in readme
    assert readme.index("## Data\n") < readme.index("## Changelog\n")
    assert readme.rstrip().endswith(
        "## Advanced download options\n\n"
        "- Catalog page:\n  - https://catalog.ourworldindata.org/energy/owid_energy/\n"
        "- energy\n"
        "  - Links to the latest data. They always give you the newest release:\n"
        "    - https://catalog.ourworldindata.org/energy/owid_energy/energy.csv\n"
        "    - https://catalog.ourworldindata.org/energy/owid_energy/energy.xlsx\n"
        "    - https://catalog.ourworldindata.org/energy/owid_energy/energy.parquet\n"
        "    - https://catalog.ourworldindata.org/energy/owid_energy/energy.codebook.csv\n"
        "    - https://catalog.ourworldindata.org/energy/owid_energy/energy.sources.csv\n"
        "  - Links to this version (2026-09-10). They keep pointing at this version after newer releases:\n"
        "    - https://catalog.ourworldindata.org/garden/energy/2026-09-10/owid_energy/energy.feather\n"
        "    - https://catalog.ourworldindata.org/garden/energy/2026-09-10/owid_energy/energy.meta.json\n"
        "- Source code:\n  - https://github.com/owid/etl/tree/master/etl/steps/data/garden/energy/2026-09-10/"
    )
    # The page's sections, in the page's order: data (one block per table), processing, license.
    assert "## Data\n\n### energy\n\nTable: `energy`\n\n3 rows × 4 columns.\n\n#### How to cite this data\n\n" in readme
    assert (
        "#### How to cite this data\n\nEnergy Institute (2026); Population based on various sources (2024) – with minor "
        "processing by Our World in Data.\n"
    ) in readme
    assert "#### Indicators\n\n##### Country\n" in readme
    assert "##### Hydropower\n\nEnergy from hydropower.\n" in readme
    assert "Column: `hydro_energy_twh`  \nUnit: terawatt-hours (TWh)  \nDate range: 2020–2021" in readme
    # The per-capita column has no 2021 value, so its range is shorter than the table's.
    assert "Column: `hydro_energy_per_capita_kwh`  \nUnit: kilowatt-hours per person  \nDate range: 2020–2020" in readme
    assert "Sources: Energy Institute – Statistical Review of World Energy (2026)" in readme
    assert "###### How is this data described by its producer?\n\nHydroelectric output, gross." in readme
    assert "###### Notes on our processing step for this indicator\n\nConverted from exajoules." in readme
    assert readme.index("described by its producer") < readme.index("Notes on our processing step")
    assert "#### Sources\n\n##### Energy Institute – Statistical Review of World Energy (2026)" in readme
    assert "Retrieved from: https://www.energyinst.org/statistical-review/" in readme
    assert "License:" not in readme.split("## License")[0]
    assert "Citation: Energy Institute - Statistical Review of World Energy (2026)." in readme
    assert "## License\n\nOur World in Data collects and republishes this data" in readme
    assert "published under" not in readme
    assert "How to cite this dataset" not in readme
    order = ["## About this dataset", "## Data", "## How we process data", "## License", "## Advanced download options"]
    assert [readme.index(heading) for heading in order] == sorted(readme.index(heading) for heading in order)
    assert "[dataset]" not in readme
    assert "Last updated" not in readme
    # Nothing unrendered leaks through.
    assert "<%" not in readme and "<<" not in readme and "#dod:" not in readme


def test_readme_skips_jinja_templates(tmp_path):
    tb = make_table()
    tb["hydro_energy_twh"].metadata.title = "Hydropower << dimension >>"
    tb["hydro_energy_twh"].metadata.description_short = "<% if x %>Templated<% endif %>"
    ds = Dataset.create_empty(tmp_path / "ds", DatasetMeta(namespace="energy", short_name="ds"))
    ds.add(tb)
    ds.save()
    readme = ds.readme()
    assert "##### hydro_energy_twh\n" in readme
    assert "Templated" not in readme
    assert "<%" not in readme and "<<" not in readme


def test_readme_omits_excel_links_for_a_table_too_long_for_a_sheet(tmp_path, monkeypatch):
    ds = make_dataset(tmp_path / "owid_energy")
    url = "https://catalog.ourworldindata.org/energy/owid_energy/"
    assert f"    - {url}energy.xlsx\n" in ds.readme(url=url)
    # The table has three rows; with the header they no longer fit.
    monkeypatch.setattr("owid.catalog.core.docs.EXCEL_MAX_ROWS", 3)
    readme = ds.readme(url=url)
    assert ".xlsx" not in readme
    assert f"    - {url}energy.csv\n    - {url}energy.parquet\n    - {url}energy.codebook.csv\n" in readme


def test_readme_lists_the_version_files_that_exist(tmp_path):
    ds = make_dataset(tmp_path / "owid_energy")
    url = "https://catalog.ourworldindata.org/energy/owid_energy/"
    dated = "https://catalog.ourworldindata.org/garden/energy/2026-09-10/owid_energy/energy"
    # Saved as feather only: no CSV or parquet link for this version.
    readme = ds.readme(url=url)
    assert f"    - {dated}.feather\n    - {dated}.meta.json\n" in readme
    assert f"{dated}.csv" not in readme and f"{dated}.parquet" not in readme
    # A step that saves a CSV too gets a CSV link, first.
    ds.add(make_table(), formats=["feather", "csv"])
    ds.save()
    readme = ds.readme(url=url)
    assert f"    - {dated}.csv\n    - {dated}.feather\n    - {dated}.meta.json\n" in readme


def test_readme_multi_table_shows_each_table_description(tmp_path):
    ds = make_dataset(tmp_path / "owid_energy")
    extra = Table({"column": ["a"], "previous_column": ["b"]}, short_name="column_mapping").set_index("column")
    extra.metadata.title = "Column mapping"
    extra.metadata.description = "Name of each column in the previous release."
    ds.add(extra)
    ds.save()
    readme = ds.readme()
    assert "### Column mapping\n\nTable: `column_mapping`\n\nName of each column in the previous release.\n" in readme
    assert "#### Sources\n\nThis table has no external sources: its columns document the dataset itself." in readme


def test_table_to_excel_includes_sources_sheet(tmp_path):
    import openpyxl

    path = tmp_path / "table.xlsx"
    make_table().to_excel(path)
    assert openpyxl.load_workbook(path, read_only=True).sheetnames == ["data", "metadata", "sources"]


def test_citations_follow_grapher_rules():
    origins = [
        EI,
        POPULATION,
        Origin(producer="Smil", title="Energy Transitions", date_published="2017", version_producer="2nd edition"),
        Origin(producer="Bolt and van Zanden", title="Maddison Project Database", date_published="2024-04-26"),
    ]
    assert docs.attribution_label(EI) == "Energy Institute (2026)"
    assert docs.attribution_label(POPULATION) == "Population based on various sources (2024)"
    # More than three attributions are shortened to the first one.
    assert (
        docs.citation_short(origins, processing_level="major")
        == "Energy Institute (2026) and other sources – with major processing by Our World in Data"
    )
    assert docs.citation_short(origins[:2], processing_level="minor") == (
        "Energy Institute (2026); Population based on various sources (2024) – with minor processing by Our World in Data"
    )
