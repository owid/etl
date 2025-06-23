"""
Generated TypedDict schemas for Collection model.

This file is auto-generated from JSON schemas. Do not edit manually.
Run `python scripts/generate_schema_types.py` to regenerate.

Provides strongly-typed interfaces for:
- View configuration (based on multidim-schema.json)
- View metadata (based on dataset-schema.json)
"""

from typing import Any, Literal, TypedDict

# =============================================================================
# Fallback Types for Undefined Nested Objects
# =============================================================================

# Fallback for nested types that reference other nested types not yet generated
GlobeConfig = dict[str, Any]
FaqsConfig = dict[str, Any]
GrapherConfig = dict[str, Any]


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
    # Select a specific time to be displayed.
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
    yEquals: str


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
            "Working Hours",
        ]
    ]


# =============================================================================
# View Configuration Types
# =============================================================================


class _ViewConfigBase(TypedDict, total=False):
    """Base ViewConfig without the $schema field."""

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
        ]
    ]
    colorScale: dict[str, Any]
    compareEndPointsOnly: bool
    comparisonLines: list[ComparisonLinesConfig]
    entityType: str
    entityTypePlural: str
    excludedEntityNames: list[str]
    facettingLabelByYVariables: str
    focusedSeriesNames: list[str]
    hasMapTab: bool
    hideAnnotationFieldsInTitle: HideAnnotationFieldsInTitleConfig
    hideConnectedScatterLines: bool
    hideFacetControl: bool
    hideLegend: bool
    hideLogo: bool
    hideRelativeToggle: bool
    hideScatterLabels: bool
    hideTimeline: bool
    hideTotalValueLabel: bool
    includedEntityNames: list[str]
    internalNotes: str
    invertColorScheme: bool
    logo: Literal["owid", "core+owid", "gv+owid"]
    map: MapConfig
    matchingEntitiesOnly: bool
    maxTime: dict[str, Any]
    minTime: dict[str, Any]
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
    sortBy: Literal["column", "total", "entityName", "custom"]
    sortColumnSlug: str
    sortOrder: Literal["desc", "asc"]
    sourceDesc: str
    stackMode: Literal["absolute", "relative", "grouped", "stacked"]
    subtitle: str
    tab: Literal["chart", "map", "table", "line", "slope"]
    timelineMaxTime: dict[str, Any]
    timelineMinTime: dict[str, Any]
    title: str
    variantName: str
    version: int
    xAxis: dict[str, Any]
    yAxis: dict[str, Any]
    zoomToSelection: bool


class ViewConfig(_ViewConfigBase, total=False):
    """View configuration options based on multidim schema."""

    pass


# Add the $schema field using __annotations__ to avoid syntax issues
ViewConfig.__annotations__.update({"$schema": str})


# =============================================================================
# View Metadata Types
# =============================================================================


class ViewMetadata(TypedDict, total=False):
    """View metadata options based on dataset schema."""

    # Description of the indicator written by the producer, if any was given.
    description_from_producer: str
    # List of key pieces of information about the indicator.
    description_key: list[str | list[str]]
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
    processing_level: Any | str
    # Characters that represent the unit we use to measure the indicator value.
    short_unit: str
    sort: list[str]
    # List of all sources of the indicator. Automatically filled. NOTE: This is no longer in use, you should use origins.
    sources: list[Any]
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


# =============================================================================
# Collection Method Parameter Types
# =============================================================================


class _GroupViewsConfigRequired(TypedDict):
    """Required fields for GroupViewsConfig."""

    dimension: str  # Slug of the dimension that contains the choices to group
    choice_new_slug: str  # The slug for the newly created choice


class GroupViewsConfig(_GroupViewsConfigRequired, total=False):
    """Configuration for group_views method parameters."""

    # Optional fields
    choices: list[str]  # Slugs of the choices to group. If not provided, all choices are used
    view_config: ViewConfigParam  # The view config for the new choice
    view_metadata: ViewMetadataParam  # The metadata for the new view
    replace: bool  # If True, the original choices will be removed and replaced with the new choice
    overwrite_dimension_choice: (
        bool  # If True and choice_new_slug already exists, views created here will overwrite existing ones
    )


GroupViewsParam = GroupViewsConfig | dict[str, Any]


def create_group_config(
    dimension: str,
    choice_new_slug: str,
    *,
    choices: list[str] | None = None,
    view_config: ViewConfigParam | None = None,
    view_metadata: ViewMetadataParam | None = None,
    replace: bool = False,
    overwrite_dimension_choice: bool = False,
) -> GroupViewsConfig:
    """Helper function to create a GroupViewsConfig with proper typing and autocompletion.

    Args:
        dimension: Slug of the dimension that contains the choices to group
        choice_new_slug: The slug for the newly created choice
        choices: Slugs of the choices to group. If not provided, all choices are used
        view_config: The view config for the new choice
        view_metadata: The metadata for the new view
        replace: If True, the original choices will be removed and replaced with the new choice
        overwrite_dimension_choice: If True and choice_new_slug already exists, views created here will overwrite existing ones

    Returns:
        A properly typed GroupViewsConfig dictionary

    Example:
        >>> config = create_group_config(
        ...     dimension="sex",
        ...     choice_new_slug="combined",
        ...     choices=["male", "female"],
        ...     view_config={"title": "Combined view"}
        ... )
    """
    result: GroupViewsConfig = {
        "dimension": dimension,
        "choice_new_slug": choice_new_slug,
    }

    if choices is not None:
        result["choices"] = choices
    if view_config is not None:
        result["view_config"] = view_config
    if view_metadata is not None:
        result["view_metadata"] = view_metadata
    if replace:
        result["replace"] = replace
    if overwrite_dimension_choice:
        result["overwrite_dimension_choice"] = overwrite_dimension_choice

    return result
