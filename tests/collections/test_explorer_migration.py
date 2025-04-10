import re

import pytest

from etl.collections.explorer_migration import migrate_csv_explorer
from etl.files import yaml_dump
from etl.helpers import PathFinder
from etl.paths import STEP_DIR

influenza = """
explorerTitle	Influenza
explorerSubtitle	Explore global data produced by the World Health Organization on influenza symptoms and cases.
selection	Northern Hemisphere	Southern Hemisphere
isPublished	true
thumbnail
hideAlertBanner	true
hideAnnotationFieldsInTitle	true
yAxisMin	0
wpBlockId	56732
hasMapTab	true
pickerColumnSlugs	Country
downloadDataLink
graphers
	title	subtitle	ySlugs	tableSlug	type	Confirmed cases or Symptoms Radio	Metric Dropdown	Interval Radio	Surveillance type Dropdown	timelineMinTime	selectedFacetStrategy	facetYDomain	hasMapTab	note	defaultView	tab	relatedQuestionText	relatedQuestionUrl
	Weekly reported cases of acute respiratory infections	Acute...	reported_ari_cases	flu_weekly	LineChart	Symptoms	Acute respiratory infections	Weekly		-4043
	Monthly reported cases of acute respiratory infections	Acute...	reported_ari_cases	flu_monthly	LineChart	Symptoms	Acute respiratory infections	Monthly		-4043
table	https://catalog.ourworldindata.org/explorers/who/latest/flu/flu.csv	flu_weekly
columns	flu_weekly
	slug	name	type	tolerance	sourceName	additionalInfo	colorScaleNumericMinValue	color	colorScaleScheme	colorScaleNumericBins	unit	shortUnit	sourceLink	dataPublishedBy
	country	Country	EntityName				0
	date	Day	Date				0
	reported_ari_cases	Cases of acute respiratory infections	Integer	30	FluID by the World Health Organization (2023)	**Dataset Description**:\\n- FluID...release.	0		YlOrRd		reported cases		https://source	Global...
table	https://catalog.ourworldindata.org/explorers/who/latest/flu/flu_monthly.csv	flu_monthly
columns	flu_monthly
	slug	name	type	tolerance	sourceName	additionalInfo	colorScaleNumericMinValue	color	colorScaleScheme	colorScaleNumericBins	unit	shortUnit	sourceLink	dataPublishedBy
	country	Country	EntityName				0
	month_date	Month	Date				0
	reported_ari_cases	Reported cases of acute respiratory infections	Integer	30	FluID by the World Health Organization (2023)	**Dataset Description:**\\n- FluID...	0		YlOrRd		reported cases		https://source	Global...
"""

expected_influenza = """
config:
  explorerTitle: Influenza
  explorerSubtitle: Explore global data produced by the World Health Organization on influenza symptoms and cases.
  selection:
    - Northern Hemisphere
    - Southern Hemisphere
  isPublished: true
  hideAlertBanner: true
  hideAnnotationFieldsInTitle: true
  yAxisMin: 0
  wpBlockId: 56732
  hasMapTab: true
  pickerColumnSlugs:
    - Country
dimensions:
  - slug: confirmed_cases_or_symptoms
    name: Confirmed cases or Symptoms
    choices:
      - slug: symptoms
        name: Symptoms
    presentation:
      type: radio
  - slug: metric
    name: Metric
    choices:
      - slug: acute_respiratory_infections
        name: Acute respiratory infections
    presentation:
      type: dropdown
  - slug: interval
    name: Interval
    choices:
      - slug: weekly
        name: Weekly
      - slug: monthly
        name: Monthly
    presentation:
      type: radio
views:
  - dimensions:
      confirmed_cases_or_symptoms: symptoms
      metric: acute_respiratory_infections
      interval: weekly
    indicators:
      y:
        - catalogPath: flu#reported_ari_cases
          display:
            name: Cases of acute respiratory infections
            type: Integer
            tolerance: 30
            sourceName: FluID by the World Health Organization (2023)
            additionalInfo: '**Dataset Description**:\\n- FluID...release.'
            colorScaleNumericMinValue: 0
            colorScaleScheme: YlOrRd
            unit: reported cases
            sourceLink: https://source
            dataPublishedBy: Global...
    config:
      title: Weekly reported cases of acute respiratory infections
      subtitle: Acute...
      type: LineChart
      timelineMinTime: -4043
  - dimensions:
      confirmed_cases_or_symptoms: symptoms
      metric: acute_respiratory_infections
      interval: monthly
    indicators:
      y:
        - catalogPath: flu_monthly#reported_ari_cases
          display:
            name: Reported cases of acute respiratory infections
            type: Integer
            tolerance: 30
            sourceName: FluID by the World Health Organization (2023)
            additionalInfo: '**Dataset Description:**\\n- FluID...'
            colorScaleNumericMinValue: 0
            colorScaleScheme: YlOrRd
            unit: reported cases
            sourceLink: https://source
            dataPublishedBy: Global...
    config:
      title: Monthly reported cases of acute respiratory infections
      subtitle: Acute...
      type: LineChart
      timelineMinTime: -4043
""".strip()


def test_migrate_csv_explorer(tmp_path):
    temp_explorer = tmp_path / "influenza.explorer.tsv"
    with open(temp_explorer, "w") as f:
        f.write(influenza)

    config = migrate_csv_explorer(temp_explorer)
    out_yaml = yaml_dump(config)

    assert out_yaml.strip() == expected_influenza


expected_tsv = """
# DO NOT EDIT THIS FILE MANUALLY. IT WAS GENERATED BY ETL step 'who/latest/influenza#influenza'.

explorerTitle	Influenza
isPublished	true
explorerSubtitle	Explore global data produced by the World Health Organization on influenza symptoms and cases.
selection	Northern Hemisphere	Southern Hemisphere
hideAlertBanner	true
hideAnnotationFieldsInTitle	true
yAxisMin	0
wpBlockId	56732
hasMapTab	true
pickerColumnSlugs	Country
graphers
	yVariableIds	ySlugs	Confirmed cases or Symptoms Radio	Metric Dropdown	Interval Radio	title	subtitle	type	timelineMinTime
		grapher__who__latest__flu__flu__reported_ari_cases__0	Symptoms	Acute respiratory infections	Weekly	Weekly reported cases of acute respiratory infections	Acute...	LineChart	-4043
		grapher__who__latest__flu__flu_monthly__reported_ari_cases__1	Symptoms	Acute respiratory infections	Monthly	Monthly reported cases of acute respiratory infections	Acute...	LineChart	-4043

columns
	catalogPath	slug	transform	additionalInfo	colorScaleNumericMinValue	colorScaleScheme	dataPublishedBy	name	sourceLink	sourceName	tolerance	type	unit
		grapher__who__latest__flu__flu__reported_ari_cases__0	duplicate IND_ID	**Dataset Description**:\\n- FluID...release.	0	YlOrRd	Global...	Cases of acute respiratory infections	https://source	FluID by the World Health Organization (2023)	30	Integer	reported cases
		grapher__who__latest__flu__flu_monthly__reported_ari_cases__1	duplicate IND_ID	**Dataset Description:**\\n- FluID...	0	YlOrRd	Global...	Reported cases of acute respiratory infections	https://source	FluID by the World Health Organization (2023)	30	Integer	reported cases
"""


@pytest.mark.integration
def test_explorer_legacy(tmp_path, monkeypatch):
    # Create TSV file
    tsv_path = tmp_path / "influenza.explorer.tsv"
    with open(tsv_path, "w") as f:
        f.write(influenza)

    # Monkeypatch ExplorerLegacy.save() to return its content
    from etl.collections.explorer_legacy import ExplorerLegacy

    d = {}

    # TODO: this is ugly, fix it when we get rid of ExplorerLegacy
    def patch_save(self, *args, **kwargs):
        d["content"] = self.content

    monkeypatch.setattr(ExplorerLegacy, "save", patch_save)

    # Dump config to YAML file
    config = migrate_csv_explorer(tsv_path)
    # Make sure explorer can deal with int values
    config["config"]["wpBlockId"] = int(config["config"]["wpBlockId"])
    config_path = tmp_path / "influenza.config.yml"
    with open(config_path, "w") as f:
        yaml_dump(config, f)

    paths = PathFinder(str(STEP_DIR / "export/explorers/who/latest/influenza"))
    assert paths._create_current_step_name() == "export://explorers/who/latest/influenza"
    paths.__dict__["dest_dir"] = tmp_path

    # Load explorer config
    config = paths.load_config(path=config_path)

    # Create explorer
    explorer = paths.create_explorer(
        config=config,
    )

    # Instead of performing the actual save, get the explorer legacy content
    explorer.save()

    # Now you can assert on content as needed
    content = d["content"]

    # Replace string "duplicate 12345" with "duplicate IND_ID"
    content = re.sub(r"duplicate\s+\d+", "duplicate IND_ID", content)

    assert content.strip() == expected_tsv.strip()
