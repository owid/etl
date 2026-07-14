"""
Generated TypedDict schemas for Collection model.

This file is auto-generated from JSON schemas. Do not edit manually.
Run `python scripts/generate_schema_types.py` to regenerate.

Provides strongly-typed interfaces for:
- View configuration (based on multidim-schema.json, resolving $refs against the
  vendored schemas/grapher-schema.010.json — refresh it with --refresh)
- View metadata (based on dataset-schema.json)
"""

from typing import Any, Literal, TypedDict

# =============================================================================
# Fallback Types for Undefined Nested Objects
# =============================================================================

# Fallback for nested types that reference other nested types not yet generated
FaqsConfig = dict[str, Any]
GlobeConfig = dict[str, Any]
GrapherConfig = dict[str, Any]
TrendColorMapConfig = dict[str, Any]


# =============================================================================
# Nested Configuration Types
# =============================================================================


class RelatedQuestionsConfig(TypedDict, total=False):
    """Nested configuration for RelatedQuestionsConfig."""

    text: str
    url: str


class MapConfig(TypedDict, total=False):
    """Nested configuration for MapConfig."""

    colorScale: dict[str, Any]
    # Column to show in the map tab. Can be a column slug (e.g. in explorers) or a variable ID (as string).
    # If not provided, the first y dimension is used.
    columnSlug: str
    # Configuration of the globe
    globe: GlobeConfig
    # Whether the timeline should be hidden in the map view and thus the user not be able to change the year
    hideTimeline: bool
    # Which region to focus on
    region: Literal["World", "Europe", "Africa", "Asia", "NorthAmerica", "SouthAmerica", "Oceania"]
    # The initial selection of entities to show on the map
    selectedEntityNames: list[str]
    # Select a specific start time to be displayed. If given, two maps are shown next to each other.
    startTime: float | Literal["latest", "earliest"]
    # Select a specific time to be displayed. If startTime is given, this acts as the end time.
    time: float | Literal["latest", "earliest"]
    # Tolerance to use. If data points are missing for a time point, a match is accepted if it lies
    # within the specified time period. The unit is the dominating time unit, usually years but can be days for
    # daily time series. If not provided, the tolerance specified in the metadata of the indicator is used.
    # If that's not specified, 0 is used.
    timeTolerance: int
    # Tolerance strategy to use. Options include accepting matches that are "closest" to the time point in question
    # (going forwards and backwards in time), and accepting matches that lie in the past ("backwards") or
    # the future ("forwards").
    toleranceStrategy: Literal["closest", "forwards", "backwards"]
    # Show the label from colorSchemeLabels in the tooltip instead of the numeric value
    tooltipUseCustomLabels: bool


class ComparisonLinesConfig(TypedDict, total=False):
    """Nested configuration for ComparisonLinesConfig."""

    label: str
    xEquals: float
    yEquals: str


class DumbbellConfig(TypedDict, total=False):
    """Nested configuration for DumbbellConfig."""

    # How the connector between dumbbell heads is drawn.
    # - arrow: draw an arrow pointing from start to end
    # - line: draw a simple line (only respected when two indicators are plotted)
    connectorStyle: Literal["arrow", "line"]
    # Custom colors for the time-range encoding
    trendColorMap: TrendColorMapConfig
    # What to display as value labels next to each dumbbell.
    # - absolute: show the raw values at the start and end
    # - change: show the absolute change (e.g. +50)
    # - percentChange: show the percentage change (e.g. +33%)
    # - none: hide value labels
    valueLabelMode: Literal["absolute", "change", "percentChange", "none"]


class HideAnnotationFieldsInTitleConfig(TypedDict, total=False):
    """Nested configuration for HideAnnotationFieldsInTitleConfig."""

    # Whether to hide "Change in" in relative line charts
    changeInPrefix: bool
    # Whether to hide the entity annotation
    entity: bool
    # Whether to hide the time annotation
    time: bool


class PresentationLicenseConfig(TypedDict, total=False):
    """Nested configuration for PresentationLicenseConfig."""

    name: str
    url: str


class PresentationConfig(TypedDict, total=False):
    """Nested configuration for PresentationConfig."""

    # Citation of the indicator's origins, to override the automatic format `producer1 (year1); producer2 (year2)`.
    attribution: str
    # Very short citation of the indicator's main producer(s).
    attribution_short: str
    # List of references to questions in an FAQ google document, relevant to the indicator.
    faqs: list[FaqsConfig]
    # Our World in Data grapher configuration. Most of the fields can be left empty and will be filled with reasonable default values.
    # Find more details on its attributes [here](https://files.ourworldindata.org/schemas/grapher-schema.003.json).
    grapher_config: GrapherConfig
    # Indicator title to be shown in public places like data pages, that overrides the indicator's title.
    title_public: str
    # Short disambiguation of the title that references a special feature of the methods or nature of the data.
    title_variant: str
    # List of topics where the indicator is relevant.
    topic_tags: list[
        Literal[
            "Access to Energy",
            "Age Structure",
            "Agricultural Production",
            "Air Pollution",
            "Alcohol Consumption",
            "Animal Welfare",
            "Antibiotics & Antibiotic Resistance",
            "Artificial Intelligence",
            "Biodiversity",
            "Books",
            "Burden of Disease",
            "CO2 & Greenhouse Gas Emissions",
            "COVID-19",
            "Cancer",
            "Cardiovascular Diseases",
            "Causes of Death",
            "Child & Infant Mortality",
            "Child Labor",
            "Clean Water",
            "Clean Water & Sanitation",
            "Climate Change",
            "Corruption",
            "Crop Yields",
            "Democracy",
            "Diarrheal Diseases",
            "Diet Compositions",
            "Economic Growth",
            "Economic Inequality",
            "Economic Inequality by Gender",
            "Education Spending",
            "Electricity Mix",
            "Employment in Agriculture",
            "Energy",
            "Energy Mix",
            "Environmental Impacts of Food Production",
            "Eradication of Diseases",
            "Famines",
            "Farm Size",
            "Fertility Rate",
            "Fertilizers",
            "Fish & Overfishing",
            "Food Prices",
            "Food Supply",
            "Foreign Aid",
            "Forests & Deforestation",
            "Fossil Fuels",
            "Gender Ratio",
            "Global Education",
            "Global Health",
            "Government Spending",
            "HIV/AIDS",
            "Happiness & Life Satisfaction",
            "Healthcare Spending",
            "Homelessness",
            "Homicides",
            "Housing",
            "Human Development Index (HDI)",
            "Human Height",
            "Human Rights",
            "Hunger & Undernourishment",
            "Illicit Drug Use",
            "Indoor Air Pollution",
            "Influenza",
            "Internet",
            "LGBT+ Rights",
            "Land Use",
            "Lead Pollution",
            "Life Expectancy",
            "Light at Night",
            "Literacy",
            "Loneliness & Social Connections",
            "Malaria",
            "Marriages & Divorces",
            "Maternal Mortality",
            "Meat & Dairy Production",
            "Medicine & Biotechnology",
            "Mental Health",
            "Metals & Minerals",
            "Micronutrient Deficiency",
            "Migration",
            "Military Personnel & Spending",
            "Mpox (monkeypox)",
            "Natural Disasters",
            "Neglected Tropical Diseases",
            "Nuclear Energy",
            "Nuclear Weapons",
            "Obesity",
            "Oil Spills",
            "Outdoor Air Pollution",
            "Ozone Layer",
            "Pandemics",
            "Pesticides",
            "Plastic Pollution",
            "Pneumonia",
            "Polio",
            "Population Growth",
            "Poverty",
            "Religion",
            "Renewable Energy",
            "Research & Development",
            "Sanitation",
            "Smallpox",
            "Smoking",
            "Space Exploration & Satellites",
            "State Capacity",
            "Suicides",
            "Taxation",
            "Technological Change",
            "Terrorism",
            "Tetanus",
            "Time Use",
            "Tourism",
            "Trade & Globalization",
            "Transport",
            "Trust",
            "Tuberculosis",
            "Uncategorized",
            "Urbanization",
            "Vaccination",
            "Violence Against Children & Children's Rights",
            "War & Peace",
            "Waste Management",
            "Water Use & Stress",
            "Wildfires",
            "Women's Employment",
            "Women's Rights",
            "Work & Employment",
            "Working Hours",
        ]
    ]


# =============================================================================
# View Configuration Types
# =============================================================================


class _ViewConfigBase(TypedDict, total=False):
    """Base ViewConfig without the special-character fields ($schema)."""

    addCountryMode: Literal["add-country", "change-country", "disabled"]
    baseColorScheme: dict[str, Any]
    chartTypes: list[
        Literal[
            "LineChart",
            "ScatterPlot",
            "StackedArea",
            "DiscreteBar",
            "StackedDiscreteBar",
            "SlopeChart",
            "StackedBar",
            "Marimekko",
            "Dumbbell",
        ]
    ]
    colorScale: dict[str, Any]
    compareEndPointsOnly: bool
    comparisonLines: list[ComparisonLinesConfig]
    dumbbell: DumbbellConfig
    entityType: str
    entityTypePlural: str
    excludedEntityNames: list[str]
    facettingLabelByYVariables: str
    focusedSeriesNames: list[str]
    hasMapTab: bool
    hideAnnotationFieldsInTitle: HideAnnotationFieldsInTitleConfig
    hideConnectedScatterLines: bool
    hideFacetControl: bool
    hideLogo: bool
    hideRelativeToggle: bool
    hideScatterLabels: bool
    hideSeriesLabels: bool
    hideTimeline: bool
    hideTotalValueLabel: bool
    includedEntityNames: list[str]
    internalNotes: str
    invertColorScheme: bool
    license: Literal["cc-by", "cc-by-sa", "cc-by-nc", "cc-by-nc-sa", "cc-by-nd", "cc-by-nc-nd"]
    logo: Literal["owid", "core+owid", "gv+owid"]
    map: MapConfig
    matchingEntitiesOnly: bool
    maxTime: float | Literal["latest", "earliest"]
    minTime: float | Literal["latest", "earliest"]
    missingDataStrategy: Literal["auto", "hide", "show"]
    note: str
    originUrl: str
    relatedQuestions: list[RelatedQuestionsConfig]
    scatterPointLabelStrategy: Literal["x", "y", "year"]
    selectedEntityColors: dict[str, Any]
    selectedEntityNames: list[str]
    selectedFacetStrategy: Literal["none", "entity", "metric"]
    showNoDataArea: bool
    showYearLabels: bool
    sortBy: Literal["column", "total", "entityName", "custom", "change", "startValue", "endValue"]
    sortColumnSlug: str
    sortOrder: Literal["desc", "asc"]
    sourceDesc: str
    stackMode: Literal["absolute", "relative"]
    subtitle: str
    tab: Literal[
        "chart",
        "map",
        "table",
        "line",
        "slope",
        "discrete-bar",
        "marimekko",
        "scatter",
        "stacked-area",
        "stacked-bar",
        "stacked-discrete-bar",
        "dumbbell",
    ]
    timelineMaxTime: float | Literal["latest"]
    timelineMinTime: float | Literal["earliest"]
    title: str
    variantName: str
    version: int
    xAxis: dict[str, Any]
    yAxis: dict[str, Any]
    zoomToSelection: bool

    # TODO: remove once we are done with explorers
    # Legacy ID-shortcut fields for scatter/Marimekko color/x/size dimensions. The schema
    # stores integers, but ETL `View.expand_paths` also accepts catalog-path strings
    # (short or full) which `replace_catalog_paths_with_ids` resolves to ints at upload.
    colorVariableId: int | str
    xVariableId: int | str
    sizeVariableId: int | str


class ViewConfig(_ViewConfigBase, total=False):
    """View configuration options based on multidim schema."""

    pass


# Add special-character fields using __annotations__ to avoid syntax issues
ViewConfig.__annotations__.update(
    {
        "$schema": str,
    }
)


# =============================================================================
# View Metadata Types
# =============================================================================


class ViewMetadata(TypedDict, total=False):
    """View metadata options based on dataset schema."""

    # Description of the indicator written by the producer, if any was given.
    description_from_producer: str
    # Key information about the indicator as free-form markdown text. A YAML list of bullet points is also accepted and converted to a markdown list on load.
    description_key: str | list[str | list[str]]
    # Relevant information about the processing of the indicator done by OWID.
    description_processing: str
    # One or a few lines that complement the title to have a short description of the indicator.
    description_short: str
    display: Any
    # License of the indicator, which depends on the indicator's processing level and the origins' licenses.
    license: str
    # List of all origins of the indicator.
    origins: list[Any]
    # An indicator's presentation defines how the indicator's metadata will be shown on our website (e.g. in data pages). The indicator presentation metadata fields are the attributes of the `VariablePresentationMeta`object in ETL.
    presentation: PresentationConfig
    # License to display for the indicator, overriding `license`.
    presentation_license: PresentationLicenseConfig
    # Level of processing that the indicator values have experienced.
    processing_level: Literal["minor", "major"] | str
    # Characters that represent the unit we use to measure the indicator value.
    short_unit: str
    sort: list[str]
    # Title of the indicator, which is a few words definition of the indicator.
    title: str
    # Indicator type is usually automatically inferred from the data, but must be manually set for ordinal and categorical types.
    type: Literal["float", "int", "mixed", "string", "ordinal", "categorical"]
    # Very concise name of the unit we use to measure the indicator values.
    unit: str


# =============================================================================
# Type Aliases for Method Parameters
# =============================================================================

# These provide type hints for methods that accept configuration/metadata
ViewConfigParam = ViewConfig | dict[str, Any]
ViewMetadataParam = ViewMetadata | dict[str, Any]
