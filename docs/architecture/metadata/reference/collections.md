---
tags:
  - Collections
  - Multidim
  - Explorers
---

# Collections reference

Multi-dimensional collections (MDIMs) are interactive data explorers that allow users to explore datasets across multiple dimensions. This reference documents the schema structure for defining collections.

<div class="grid cards" markdown>

- __[Collection](#collection)__ - Main collection configuration
- __[View Config](#viewconfig)__ - Chart and visualization configuration
- __[View Metadata](#viewmetadata)__ - Data presentation metadata

</div>


## `collection`



### `collection.catalog_path`

*type*: `string` | {==required==}

Unique identifier for the collection in the format 'namespace/version/dataset#name'.

=== ":fontawesome-solid-list:  Guidelines"

	- Must follow the pattern 'namespace/version/dataset#name'.
	- Must be unique across all collections.
	- Should use descriptive names that identify the collection's purpose.

=== ":material-note-edit: Examples"

	| :material-check: DO      | :material-close: DON'T  |
	| ----------- | --------- |
	| «`energy/2024-01-01/energy_mix#renewable_energy`» | «`energy`», «`energy/2024-01-01`», «`energy/2024-01-01/energy_mix`» |
	| «`demography/2023-12-15/population#age_structure`» | «`demography`», «`population#age_structure`» |



---


### `collection.default_dimensions`

Mapping of dimension slugs to their default choice slugs for the initial view.

### `collection.default_selection`

*type*: `array` | {==required==}

List of entity names (e.g., countries) selected by default when the collection loads.

=== ":fontawesome-solid-list:  Guidelines"

	- Must contain at least one entity.
	- Entity names must be harmonized country names from the OWID catalog.
	- Should include 3-8 entities for optimal visualization.
	- Should represent a diverse and meaningful set of entities.

=== ":material-note-edit: Examples"

	| :material-check: DO      | :material-close: DON'T  |
	| ----------- | --------- |
	| «`["United States", "China", "Germany", "India"]`» | «`[]`», «`["USA", "PRC"]`» |
	| «`["World"]`» | «`["Earth"]`», «`["Global"]`» |
	| «`["Europe", "North America", "Asia"]`» | «`["EU", "NA", "AS"]`», «`["Too many entities..."]`» |



---


### `collection.definitions`

Common definitions and configurations that can be referenced across multiple views.

### `collection.dimensions`

*type*: `array` | {==required==}

Array of dimension objects that define the filter dropdowns available to users.

=== ":fontawesome-solid-list:  Guidelines"

	- Must contain at least one dimension.
	- Each dimension creates a dropdown filter in the user interface.
	- Dimension slugs must be unique within the collection.
	- Should be ordered by importance or logical flow.




---


### `collection.grapherConfigSchema`

*type*: `string` | optional

URL pointing to the JSON schema that validates the grapher configuration.

=== ":fontawesome-solid-list:  Guidelines"

	- Should point to the current version of the OWID Grapher schema.
	- Used for validation of chart configuration objects.

=== ":material-note-edit: Examples"

	| :material-check: DO      | :material-close: DON'T  |
	| ----------- | --------- |
	| «`https://files.ourworldindata.org/schemas/grapher-schema.008.json`» | «`http://invalid-url.com/schema.json`», «`grapher-schema.008.json`» |



---


### `collection.metadata`



#### `collection.metadata.description_from_producer`

*type*: `string` | recommended (if existing)

Description of the indicator written by the producer, if any was given.

=== ":fontawesome-solid-list:  Guidelines"

	- Must start with a capital letter.
	- Must end with a period.
	- Should be identical to the producer's text, except for some formatting changes, typo corrections, or other appropriate minor edits.
	- Should only be given if the producer clearly provides such definitions in a structured way. Avoid spending time searching for a definition given by the producer elsewhere.




---


#### `collection.metadata.description_key`

*type*: `array` | recommended (for curated indicators)

List of key pieces of information about the indicator.

=== ":fontawesome-solid-list:  Guidelines"

	- Must be a list of one or more short paragraphs.
		- Each paragraph must start with a capital letter.
		- Each paragraph must end with a period.
	- Must not contain `description_short` (although there might be some overlap of information).
	- Should contain all the key information about the indicator (except that already given in `description_short`).
	- Should include the key information given in other fields like `grapher_config.subtitle` (if different from `description_short`) and `grapher_config.note`.
	- Should not contain information about processing (which should be in `description_processing`).
	- Should only contain information that is key to the public.
		- Anything that is too detailed or technical should be left in the code.




---


#### `collection.metadata.description_processing`

*type*: `string` | {==required (if applicable)==}

Relevant information about the processing of the indicator done by OWID.

=== ":fontawesome-solid-list:  Guidelines"

	- Must start with a capital letter.
	- Must end with a period.
	- Must be used if important editorial decisions have been taken during data processing.
	- Must not be used to describe common processing steps like country harmonization.
	- Should only contain key processing information to the public.
		- Anything that is too detailed or technical should be left in the code.




---


#### `collection.metadata.description_short`

*type*: `string` | {==required==}

One or a few lines that complement the title to have a short description of the indicator.

=== ":fontawesome-solid-list:  Guidelines"

	- Must start with a capital letter.
	- Must end with a period.
	- Must be one short paragraph (for example suitable to fit in a chart subtitle).
	- Should not mention any other metadata fields (like information about the processing, or the origins, or the units). **Exceptions:**
		- The unit can be mentioned if it is crucial for the description.




---


#### `collection.metadata.display`

We keep display for the time being as the 'less powerful sibling' of grapher config.

##### `collection.metadata.display.color`

*type*: `string`

Color to use for the indicator in e.g. line charts.





---


##### `collection.metadata.display.conversionFactor`

*type*: `number`

Conversion factor to apply to indicator values.

=== ":fontawesome-solid-list:  Guidelines"

	- **Note:** We should avoid using this, and instead convert data and units (and possibly other metadata fields where the units are mentioned) consistently in the ETL grapher step.




---


##### `collection.metadata.display.description`

*type*: `string`

Description to display for the indicator, to replace the indicator's `description`.





---


##### `collection.metadata.display.entityAnnotationsMap`

*type*: `string`

Entity annotations





---


##### `collection.metadata.display.includeInTable`

*type*: `boolean`

Whether to render this indicator in the table sheet.





---


##### `collection.metadata.display.isProjection`

*type*: `boolean`, `string`, `string`

Indicates if this time series is a forward projection (if so then this is rendered differently in e.g. line charts).





---


##### `collection.metadata.display.name`

*type*: `string` | {==required==}

Indicator's title to display in the legend of a chart. NOTE: For backwards compatibility, `display.name` also replaces the indicator's title in other public places. Therefore, whenever `display.name` is defined, `title_public` should also be defined.

=== ":fontawesome-solid-list:  Guidelines"

	- Must be very short, to fit the legend of a chart.
	- Must not end with a period.
	- Should not mention other metadata fields like `producer` or `version`.

=== ":material-note-edit: Examples"

	| :material-check: DO      | :material-close: DON'T  |
	| ----------- | --------- |
	| «`Agriculture`» | «`Nitrous oxide emissions from agriculture`» |
	| «`Area harvested`» | «`Barley | 00000044 || Area harvested | 005312 || hectares`» |



---


##### `collection.metadata.display.numDecimalPlaces`

*type*: `integer`, `string`, `string`

Number of decimal places to show in charts (and in the table tab).





---


##### `collection.metadata.display.numSignificantFigures`

*type*: `integer`, `string`

Number of significant rounding figures in charts.





---


##### `collection.metadata.display.roundingMode`

*type*: `string`

Specifies the rounding mode to use.





---


##### `collection.metadata.display.shortUnit`

*type*: `string`

Short unit to use in charts instead of the indicator's `short_unit`.





---


##### `collection.metadata.display.tableDisplay`

Configuration for the table tab for this indicator, with options `hideAbsoluteChange` and `hideRelativeChange`.

###### `collection.metadata.display.tableDisplay.hideAbsoluteChange`

*type*: `boolean`

Whether to hide the absolute change.





---


###### `collection.metadata.display.tableDisplay.hideRelativeChange`

*type*: `boolean`

Whether to hide the relative change.





---


##### `collection.metadata.display.tolerance`

*type*: `integer`

Tolerance (in years or days) to use in charts. If data points are missing, the closest data point will be shown, if it lies within the specified tolerance.





---


##### `collection.metadata.display.unit`

*type*: `string`

Unit to use in charts instead of the indicator's `unit`.





---


##### `collection.metadata.display.yearIsDay`

*type*: `boolean`

Switch to indicate if the number in the year column represents a day (since zeroDay) or a year.





---


##### `collection.metadata.display.zeroDay`

*type*: `string`

ISO date day string for the starting date if `yearIsDay` is `True`.





---


#### `collection.metadata.license`

*type*: `string` | {==required (in the future this could be automatic)==}

License of the indicator, which depends on the indicator's processing level and the origins' licenses.

=== ":fontawesome-solid-list:  Guidelines"

	- If the indicator's `processing_level` is major, assign `CC BY 4.0`.
	- If the indicator's `processing_level` is minor, choose the most strict license among the origins' `licenses`.




---


#### `collection.metadata.origins`

*type*: `array`

List of all origins of the indicator.

=== ":fontawesome-solid-list:  Guidelines"

	- **Note:** Origins should be propagated automatically from snapshots. Therefore, this field should only be manually filled out if automatic propagation fails.




---


#### `collection.metadata.presentation`

An indicator's presentation defines how the indicator's metadata will be shown on our website (e.g. in data pages). The indicator presentation metadata fields are the attributes of the `VariablePresentationMeta`object in ETL.

##### `collection.metadata.presentation.attribution`

*type*: `string` | optional

Citation of the indicator's origins, to override the automatic format `producer1 (year1); producer2 (year2)`.

=== ":fontawesome-solid-list:  Guidelines"

	- Must start with a capital letter. **Exceptions:**
		- The name of the institution or the author must be spelled with small letter, e.g. `van Haasteren`.
	- Must join multiple attributions by a `;`.
	- Must not end in a period (and must **not** end in `;`).
	- Must contain the year of `date_published`, for each origin, in parenthesis.
	- Should only be used when the automatic format `producer1 (year1); producer2 (year2)` needs to be overridden.

=== ":material-note-edit: Examples"

	| :material-check: DO      | :material-close: DON'T  |
	| ----------- | --------- |
	| «`Energy Institute - Statistical Review of World Energy (2023); Ember (2022)`» | «`UN (2023), WHO (2023)`» |



---


##### `collection.metadata.presentation.attribution_short`

*type*: `string` | recommended (for curated indicators)

Very short citation of the indicator's main producer(s).

=== ":fontawesome-solid-list:  Guidelines"

	- Must start with a capital letter. **Exceptions:**
		- The name of the institution or the author must be spelled with small letter, e.g. `van Haasteren`.
	- Must not end in a period.
	- Should be very short.
	- Should be used if the automatic concatenation of origin's `attribution_short` are too long. In those cases, choose the most important `attribution` (e.g. the main producer of the data).




---


##### `collection.metadata.presentation.faqs`

*type*: `array` | recommended (for curated indicators)

List of references to questions in an FAQ google document, relevant to the indicator.

=== ":fontawesome-solid-list:  Guidelines"

	- Each reference must contain `fragment_id` (question identifier) and `gdoc_id` (document identifier).




---


##### `collection.metadata.presentation.grapher_config`

Our World in Data grapher configuration. Most of the fields can be left empty and will be filled with reasonable default values.

Find more details on its attributes [here](https://files.ourworldindata.org/schemas/grapher-schema.003.json).

###### `collection.metadata.presentation.grapher_config.$schema`

*type*: `string`

Url of the concrete schema version to use to validate this document





---


###### `collection.metadata.presentation.grapher_config.addCountryMode`

*type*: `string`

Whether the user can change countries, add additional ones or neither





---


###### `collection.metadata.presentation.grapher_config.baseColorScheme`

*type*: `string`

The default color scheme if no color overrides are specified





---


###### `collection.metadata.presentation.grapher_config.chartTypes`

*type*: `array`

Which types of chart should be shown





---


###### `collection.metadata.presentation.grapher_config.colorScale`

Color scale definition

####### `collection.metadata.presentation.grapher_config.colorScale.baseColorScheme`

*type*: `['YlGn', 'YlGnBu', 'GnBu', 'BuGn', 'PuBuGn', 'BuPu', 'RdPu', 'PuRd', 'OrRd', 'YlOrRd', 'YlOrBr', 'Purples', 'Blues', 'Greens', 'Oranges', 'Reds', 'Greys', 'PuOr', 'BrBG', 'PRGn', 'PiYG', 'RdBu', 'RdGy', 'RdYlBu', 'Spectral', 'RdYlGn', 'Accent', 'Dark2', 'Paired', 'Pastel1', 'Pastel2', 'Set1', 'Set2', 'Set3', 'PuBu', 'hsv-RdBu', 'hsv-CyMg', 'Magma', 'Inferno', 'Plasma', 'Viridis', 'continents', 'stackedAreaDefault', 'owid-distinct', 'default', 'ContinentsLines', 'SingleColorDenim', 'SingleColorTeal', 'SingleColorPurple', 'SingleColorDustyCoral', 'SingleColorDarkCopper', 'OwidCategoricalA', 'OwidCategoricalB', 'OwidCategoricalC', 'OwidCategoricalD', 'OwidCategoricalE', 'OwidEnergy', 'OwidEnergyLines', 'OwidDistinctLines', 'BinaryMapPaletteA', 'BinaryMapPaletteB', 'BinaryMapPaletteC', 'BinaryMapPaletteD', 'BinaryMapPaletteE', 'SingleColorGradientDenim', 'SingleColorGradientTeal', 'SingleColorGradientPurple', 'SingleColorGradientDustyCoral', 'SingleColorGradientDarkCopper']`, `string`, `string`, `array`

One of the predefined base color schemes





---


####### `collection.metadata.presentation.grapher_config.colorScale.binningStrategy`

*type*: `string`

The strategy for generating the bin boundaries





---


####### `collection.metadata.presentation.grapher_config.colorScale.binningStrategyBinCount`

*type*: `integer`

The *suggested* number of bins for the automatic binning algorithm





---


####### `collection.metadata.presentation.grapher_config.colorScale.colorSchemeInvert`

*type*: `boolean`

Reverse the order of colors in the color scheme





---


####### `collection.metadata.presentation.grapher_config.colorScale.customCategoryColors`

Map of categorical values to colors. Colors are CSS colors, usually in the form `#aa9944`

####### `collection.metadata.presentation.grapher_config.colorScale.customCategoryLabels`

Map of category values to color legend labels.

####### `collection.metadata.presentation.grapher_config.colorScale.customHiddenCategories`

Allow hiding categories from the legend

####### `collection.metadata.presentation.grapher_config.colorScale.customNumericColors`

*type*: `array`

Override some or all colors for the numerical color legend.
Colors are CSS colors, usually in the form `#aa9944`
`null` falls back the color scheme color.






---


####### `collection.metadata.presentation.grapher_config.colorScale.customNumericColorsActive`

*type*: `boolean`

Whether `customNumericColors` are used to override the color scheme





---


####### `collection.metadata.presentation.grapher_config.colorScale.customNumericLabels`

*type*: `array`

Custom labels for each numeric bin. Only applied when strategy is `manual`.
`null` falls back to default label.






---


####### `collection.metadata.presentation.grapher_config.colorScale.customNumericMinValue`

*type*: `number`, `string`

The minimum bracket of the first bin





---


####### `collection.metadata.presentation.grapher_config.colorScale.customNumericValues`

*type*: `array`, `string`

Custom maximum brackets for each numeric bin. Only applied when strategy is `manual` or when using a string matching pattern like `<%`.





---


####### `collection.metadata.presentation.grapher_config.colorScale.legendDescription`

*type*: `string`

A custom legend description. Only used in ScatterPlot legend titles for now.





---


###### `collection.metadata.presentation.grapher_config.compareEndPointsOnly`

*type*: `boolean`

Drops in between points in scatter plots





---


###### `collection.metadata.presentation.grapher_config.comparisonLines`

*type*: `array`

List of vertical comparison lines to draw





---


###### `collection.metadata.presentation.grapher_config.data`

Obsolete name - used only to store the available entities

####### `collection.metadata.presentation.grapher_config.data.availableEntities`

*type*: `array`

List of available entities





---


###### `collection.metadata.presentation.grapher_config.dimensions`

*type*: `array`

List of dimensions and their mapping to variables





---


###### `collection.metadata.presentation.grapher_config.entityType`

*type*: `string`

Display string for naming the primary entities of the data. Default is 'country or region', but you can specify a different one such as 'state' or 'region'





---


###### `collection.metadata.presentation.grapher_config.entityTypePlural`

*type*: `string`

Plural of the entity type (i.e. when entityType is 'country' this would be 'countries')





---


###### `collection.metadata.presentation.grapher_config.excludedEntityNames`

*type*: `array`

Entity names that should be excluded from the chart





---


###### `collection.metadata.presentation.grapher_config.facettingLabelByYVariables`

*type*: `string`

Display string that replaces 'metric' in the 'Split by metric' label in facet controls (e.g. 'product' displays 'Split by product')





---


###### `collection.metadata.presentation.grapher_config.hasMapTab`

*type*: `boolean`, `string`

Whether the default chart for the indicator should include a map tab.





---


###### `collection.metadata.presentation.grapher_config.hideAnnotationFieldsInTitle`

Whether to hide any automatically added title annotations like the selected year

####### `collection.metadata.presentation.grapher_config.hideAnnotationFieldsInTitle.changeInPrefix`

*type*: `boolean`

Whether to hide "Change in" in relative line charts





---


####### `collection.metadata.presentation.grapher_config.hideAnnotationFieldsInTitle.entity`

*type*: `boolean`

Whether to hide the entity annotation





---


####### `collection.metadata.presentation.grapher_config.hideAnnotationFieldsInTitle.time`

*type*: `boolean`

Whether to hide the time annotation





---


###### `collection.metadata.presentation.grapher_config.hideConnectedScatterLines`

*type*: `boolean`

Whether to hide connecting lines on scatter plots when a time range is selected





---


###### `collection.metadata.presentation.grapher_config.hideFacetControl`

*type*: `boolean`

Whether to hide the faceting control





---


###### `collection.metadata.presentation.grapher_config.hideLegend`

*type*: `boolean`

Hide legend in chart.





---


###### `collection.metadata.presentation.grapher_config.hideLogo`

*type*: `boolean`

Hide logo in chart.





---


###### `collection.metadata.presentation.grapher_config.hideRelativeToggle`

*type*: `boolean`

Whether to hide the relative mode UI toggle. Default depends on the chart type





---


###### `collection.metadata.presentation.grapher_config.hideScatterLabels`

*type*: `boolean`

Hide entity names in Scatter plots





---


###### `collection.metadata.presentation.grapher_config.hideTimeline`

*type*: `boolean`

Whether to hide the timeline from the user. If it is hidden then the user can't change the time





---


###### `collection.metadata.presentation.grapher_config.hideTotalValueLabel`

*type*: `boolean`

Whether to hide the total value label (used on stacked discrete bar charts)





---


###### `collection.metadata.presentation.grapher_config.id`

*type*: `integer`

Internal DB id. Useful internally for OWID but not required if just using grapher directly.





---


###### `collection.metadata.presentation.grapher_config.includedEntities`

*type*: `array`

Entity names to include. Opposite of includedEntityNames. If this is set then all entities not specified here are excluded.





---


###### `collection.metadata.presentation.grapher_config.internalNotes`

*type*: `string`

Internal notes.





---


###### `collection.metadata.presentation.grapher_config.invertColorScheme`

*type*: `boolean`

Reverse the order of colors in the color scheme





---


###### `collection.metadata.presentation.grapher_config.isPublished`

*type*: `boolean`

Indicates if the chart is published on Our World in Data or still in draft





---


###### `collection.metadata.presentation.grapher_config.logo`

*type*: `string`

Which logo to show on the upper right side





---


###### `collection.metadata.presentation.grapher_config.map`

Configuration of the world map chart

####### `collection.metadata.presentation.grapher_config.map.colorScale`

Color scale definition

######## `collection.metadata.presentation.grapher_config.map.colorScale.baseColorScheme`

*type*: `['YlGn', 'YlGnBu', 'GnBu', 'BuGn', 'PuBuGn', 'BuPu', 'RdPu', 'PuRd', 'OrRd', 'YlOrRd', 'YlOrBr', 'Purples', 'Blues', 'Greens', 'Oranges', 'Reds', 'Greys', 'PuOr', 'BrBG', 'PRGn', 'PiYG', 'RdBu', 'RdGy', 'RdYlBu', 'Spectral', 'RdYlGn', 'Accent', 'Dark2', 'Paired', 'Pastel1', 'Pastel2', 'Set1', 'Set2', 'Set3', 'PuBu', 'hsv-RdBu', 'hsv-CyMg', 'Magma', 'Inferno', 'Plasma', 'Viridis', 'continents', 'stackedAreaDefault', 'owid-distinct', 'default', 'ContinentsLines', 'SingleColorDenim', 'SingleColorTeal', 'SingleColorPurple', 'SingleColorDustyCoral', 'SingleColorDarkCopper', 'OwidCategoricalA', 'OwidCategoricalB', 'OwidCategoricalC', 'OwidCategoricalD', 'OwidCategoricalE', 'OwidEnergy', 'OwidEnergyLines', 'OwidDistinctLines', 'BinaryMapPaletteA', 'BinaryMapPaletteB', 'BinaryMapPaletteC', 'BinaryMapPaletteD', 'BinaryMapPaletteE', 'SingleColorGradientDenim', 'SingleColorGradientTeal', 'SingleColorGradientPurple', 'SingleColorGradientDustyCoral', 'SingleColorGradientDarkCopper']`, `string`, `string`, `array`

One of the predefined base color schemes





---


######## `collection.metadata.presentation.grapher_config.map.colorScale.binningStrategy`

*type*: `string`

The strategy for generating the bin boundaries





---


######## `collection.metadata.presentation.grapher_config.map.colorScale.binningStrategyBinCount`

*type*: `integer`

The *suggested* number of bins for the automatic binning algorithm





---


######## `collection.metadata.presentation.grapher_config.map.colorScale.colorSchemeInvert`

*type*: `boolean`

Reverse the order of colors in the color scheme





---


######## `collection.metadata.presentation.grapher_config.map.colorScale.customCategoryColors`

Map of categorical values to colors. Colors are CSS colors, usually in the form `#aa9944`

######## `collection.metadata.presentation.grapher_config.map.colorScale.customCategoryLabels`

Map of category values to color legend labels.

######## `collection.metadata.presentation.grapher_config.map.colorScale.customHiddenCategories`

Allow hiding categories from the legend

######## `collection.metadata.presentation.grapher_config.map.colorScale.customNumericColors`

*type*: `array`

Override some or all colors for the numerical color legend.
Colors are CSS colors, usually in the form `#aa9944`
`null` falls back the color scheme color.






---


######## `collection.metadata.presentation.grapher_config.map.colorScale.customNumericColorsActive`

*type*: `boolean`

Whether `customNumericColors` are used to override the color scheme





---


######## `collection.metadata.presentation.grapher_config.map.colorScale.customNumericLabels`

*type*: `array`

Custom labels for each numeric bin. Only applied when strategy is `manual`.
`null` falls back to default label.






---


######## `collection.metadata.presentation.grapher_config.map.colorScale.customNumericMinValue`

*type*: `number`, `string`

The minimum bracket of the first bin





---


######## `collection.metadata.presentation.grapher_config.map.colorScale.customNumericValues`

*type*: `array`, `string`

Custom maximum brackets for each numeric bin. Only applied when strategy is `manual` or when using a string matching pattern like `<%`.





---


######## `collection.metadata.presentation.grapher_config.map.colorScale.legendDescription`

*type*: `string`

A custom legend description. Only used in ScatterPlot legend titles for now.





---


####### `collection.metadata.presentation.grapher_config.map.hideTimeline`

*type*: `boolean`

Whether the timeline should be hidden in the map view and thus the user not be able to change the year





---


####### `collection.metadata.presentation.grapher_config.map.projection`

*type*: `string`

Slightly misnamed - this does not change the map projection but instead specifies which world area to focus on.





---


####### `collection.metadata.presentation.grapher_config.map.time`

*type*: `number`, `string`

Select a specific time to be displayed.





---


####### `collection.metadata.presentation.grapher_config.map.timeTolerance`

*type*: `integer`

Tolerance to use. If data points are missing for a time point, a match is accepted if it lies
within the specified time period. The unit is the dominating time unit, usually years but can be days for
daily time series.






---


####### `collection.metadata.presentation.grapher_config.map.toleranceStrategy`

*type*: `string`

Tolerance strategy to use. Options include accepting matches that are "closest" to the time point in question
(going forwards and backwards in time), and accepting matches that lie in the past ("backwards") or
the future ("forwards").






---


####### `collection.metadata.presentation.grapher_config.map.tooltipUseCustomLabels`

*type*: `boolean`

Show the label from colorSchemeLabels in the tooltip instead of the numeric value





---


####### `collection.metadata.presentation.grapher_config.map.variableId`

*type*: `integer`

Variable ID to show. TODO: remove this and use dimensions instead





---


###### `collection.metadata.presentation.grapher_config.matchingEntitiesOnly`

*type*: `boolean`

Exclude entities that do not belong in any color group





---


###### `collection.metadata.presentation.grapher_config.maxTime`

*type*: `number`, `string`

End point of the initially selected time span.





---


###### `collection.metadata.presentation.grapher_config.minTime`

*type*: `number`, `string`

Start point of the initially selected time span.





---


###### `collection.metadata.presentation.grapher_config.missingDataStrategy`

*type*: `string`

The desired strategy for handling entities with missing data





---


###### `collection.metadata.presentation.grapher_config.note`

*type*: `string`

Default footer note to use in charts for the indicator.

=== ":fontawesome-solid-list:  Guidelines"

	- Must start with a capital letter.
	- Must end with a period.
	- Must not include 'Note:' at the beginning (this is added automatically).
	- Must be one short paragraph, usually one sentence, suitable to fit in a chart footer note.
	- Should not mention any other metadata fields.




---


###### `collection.metadata.presentation.grapher_config.originUrl`

*type*: `string`

The page containing this chart where more context can be found





---


###### `collection.metadata.presentation.grapher_config.relatedQuestions`

*type*: `array`

Links to related questions





---


###### `collection.metadata.presentation.grapher_config.scatterPointLabelStrategy`

*type*: `string`

When a user hovers over a connected series line in a ScatterPlot we show
a label for each point. By default that value will be from the "year" column
but by changing this option the column used for the x or y axis could be used instead.






---


###### `collection.metadata.presentation.grapher_config.selectedEntityColors`

Colors for selected entities

###### `collection.metadata.presentation.grapher_config.selectedEntityNames`

*type*: `array`

List of entities (e.g. harmonized country names) to use by default in charts for the indicator.





---


###### `collection.metadata.presentation.grapher_config.selectedFacetStrategy`

*type*: `string`

The desired facetting strategy (none for no facetting)





---


###### `collection.metadata.presentation.grapher_config.showNoDataArea`

*type*: `boolean`

Whether to show an area for entities that have no data (currently only used in marimekko charts)





---


###### `collection.metadata.presentation.grapher_config.showYearLabels`

*type*: `boolean`

Whether to show year labels in bar charts





---


###### `collection.metadata.presentation.grapher_config.sortBy`

*type*: `string`

Sort criterium (used by stacked bar charts and marimekko)





---


###### `collection.metadata.presentation.grapher_config.sortColumnSlug`

*type*: `string`

Sort column if sortBy is column (used by stacked bar charts and marimekko)





---


###### `collection.metadata.presentation.grapher_config.sortOrder`

*type*: `string`

Sort order (used by stacked bar charts and marimekko)





---


###### `collection.metadata.presentation.grapher_config.sourceDesc`

*type*: `string`

Short comma-separated list of source names





---


###### `collection.metadata.presentation.grapher_config.stackMode`

*type*: `string`, `null`

Stack mode. Only absolute and relative are actively used.





---


###### `collection.metadata.presentation.grapher_config.subtitle`

*type*: `string`

Default subtitle to use in charts for the indicator. NOTE: Use this field to override an indicator's `description_short` with information that is more suitable for a chart subtitle.

=== ":fontawesome-solid-list:  Guidelines"

	- Must start with a capital letter.
	- Must end with a period.
	- Must be one short paragraph, suitable to fit in a chart subtitle.




---


###### `collection.metadata.presentation.grapher_config.tab`

*type*: `['chart', 'map', 'table']`, `string`

The tab that is shown initially





---


###### `collection.metadata.presentation.grapher_config.timelineMaxTime`

*type*: `integer`

The highest year to show in the timeline. If this is set then the user is not able to see
any data after this year






---


###### `collection.metadata.presentation.grapher_config.timelineMinTime`

*type*: `integer`

The lowest year to show in the timeline. If this is set then the user is not able to see
any data before this year






---


###### `collection.metadata.presentation.grapher_config.title`

*type*: `string`

Default title to use in charts for the indicator, overriding the indicator's `title`.

=== ":fontawesome-solid-list:  Guidelines"

	- Must start with a capital letter.
	- Must not end with a period.
	- Must be one short sentence (a few words).
	- Must fit and be an appropriate choice for a chart's public title.
	- Should not mention other metadata fields like `producer` or `version`.




---


###### `collection.metadata.presentation.grapher_config.variantName`

*type*: `string`

Optional internal variant name for distinguishing charts with the same title





---


###### `collection.metadata.presentation.grapher_config.version`

*type*: `integer`







---


###### `collection.metadata.presentation.grapher_config.xAxis`

Axis definition

####### `collection.metadata.presentation.grapher_config.xAxis.canChangeScaleType`

*type*: `boolean`

Allow user to change lin/log





---


####### `collection.metadata.presentation.grapher_config.xAxis.facetDomain`

*type*: `string`

Whether the axis domain should be the same across faceted charts (if possible)





---


####### `collection.metadata.presentation.grapher_config.xAxis.label`

*type*: `string`

Axis label





---


####### `collection.metadata.presentation.grapher_config.xAxis.max`

*type*: `number`

Maximum domain value of the axis





---


####### `collection.metadata.presentation.grapher_config.xAxis.min`

*type*: `number`

Minimum domain value of the axis





---


####### `collection.metadata.presentation.grapher_config.xAxis.removePointsOutsideDomain`

*type*: `boolean`







---


####### `collection.metadata.presentation.grapher_config.xAxis.scaleType`

*type*: `string`

Toggle linear/logarithmic





---


###### `collection.metadata.presentation.grapher_config.yAxis`

Axis definition

####### `collection.metadata.presentation.grapher_config.yAxis.canChangeScaleType`

*type*: `boolean`

Allow user to change lin/log





---


####### `collection.metadata.presentation.grapher_config.yAxis.facetDomain`

*type*: `string`

Whether the axis domain should be the same across faceted charts (if possible)





---


####### `collection.metadata.presentation.grapher_config.yAxis.label`

*type*: `string`

Axis label





---


####### `collection.metadata.presentation.grapher_config.yAxis.max`

*type*: `number`

Maximum domain value of the axis





---


####### `collection.metadata.presentation.grapher_config.yAxis.min`

*type*: `number`

Minimum domain value of the axis





---


####### `collection.metadata.presentation.grapher_config.yAxis.removePointsOutsideDomain`

*type*: `boolean`







---


####### `collection.metadata.presentation.grapher_config.yAxis.scaleType`

*type*: `string`

Toggle linear/logarithmic





---


###### `collection.metadata.presentation.grapher_config.zoomToSelection`

*type*: `boolean`

Whether to zoom to the selected data points





---


##### `collection.metadata.presentation.title_public`

*type*: `string` | optional

Indicator title to be shown in public places like data pages, that overrides the indicator's title.

=== ":fontawesome-solid-list:  Guidelines"

	- Must start with a capital letter.
	- Must not end with a period.
	- Must be one short sentence (a few words).
	- Should not mention other metadata fields like `producer` or `version`.
	- Should help OWID and expert users identify the indicator.
	- For big datasets where constructing human-readable titles is hard (e.g. FAOSTAT), this field is mandatory, to improve the public appearance of the title.
	- When `display.name` is defined (to edit the default title in the legend of a chart), `title_public` must also be defined (otherwise `display.name` will be used as a public title).




---


##### `collection.metadata.presentation.title_variant`

*type*: `string` | optional

Short disambiguation of the title that references a special feature of the methods or nature of the data.

=== ":fontawesome-solid-list:  Guidelines"

	- Must start with a capital letter.
	- Must not end in a period.
	- Should be very short.
	- Should only be used if the indicator's title is ambiguous (e.g. if there are multiple indicators with the same title).
	- Should not reference the data provider. For that, instead, use `attribution_short`.

=== ":material-note-edit: Examples"

	| :material-check: DO      | :material-close: DON'T  |
	| ----------- | --------- |
	| «`Age-standardized`» | «`The data is age-standardized`» |
	| «`Historical data`» | «`Historical data from 1800 to 2010`» |



---


##### `collection.metadata.presentation.topic_tags`

*type*: `array` | recommended (for curated indicators)

List of topics where the indicator is relevant.

=== ":fontawesome-solid-list:  Guidelines"

	- Must be an existing topic tag, and be spelled correctly (see the list of topic tags in [datasette](http://analytics/private?sql=SELECT%0D%0A++DISTINCT+t.name%0D%0AFROM%0D%0A++tag_graph+tg%0D%0A++LEFT+JOIN+tags+t+ON+tg.childId+%3D+t.id%0D%0A++LEFT+JOIN+posts_gdocs+p+ON+t.slug+%3D+p.slug%0D%0A++AND+p.published+%3D+1%0D%0A++AND+p.type+IN+%28%27article%27%2C+%27topic-page%27%2C+%27linear-topic-page%27%29%0D%0AWHERE%0D%0A++p.slug+IS+NOT+NULL%0D%0AUNION%0D%0ASELECT%0D%0A++%27Uncategorized%27%0D%0AORDER+BY%0D%0A++t.name) (requires Tailscale).
	- The first tag must correspond to the most relevant topic page (since that topic page will be used in citations of this indicator).
	- Should contain 1, 2, or at most 3 tags.




---


#### `collection.metadata.presentation_license`

License to display for the indicator, overriding `license`.

##### `collection.metadata.presentation_license.name`

*type*: `string`







---


##### `collection.metadata.presentation_license.url`

*type*: `string`







---


#### `collection.metadata.processing_level`

*type*: `['minor', 'major']`, `string` | {==required (in the future this could be automatic).==}

Level of processing that the indicator values have experienced.

=== ":fontawesome-solid-list:  Guidelines"

	- Must be `minor` if the indicator has undergone only minor operations since its origin:
		- Rename entities (e.g. countries or columns)
		- Multiplication by a constant (e.g. unit change)
		- Drop missing values.
	- Must be `major` if any other operation is used:
		- Data aggregates (e.g. sum data for continents or income groups)
		- Operations between indicators (e.g. per capita, percentages, annual changes)
		- Concatenation of indicators, etc.




---


#### `collection.metadata.short_unit`

*type*: `string` | {==required==}

Characters that represent the unit we use to measure the indicator value.

=== ":fontawesome-solid-list:  Guidelines"

	- Must follow the rules of capitalization of the International System of Units, when applicable.
	- Must not end with a period.
	- Must be empty if the indicator has no units.
	- Should not contain spaces.
	- If, for clarity, we prefer to simplify the units in a chart, e.g. to show `kWh` instead of `kWh/person`, use `display.short_unit` for the simplified units, and keep the correct one in `indicator.short_unit` (and ensure there is no ambiguity in the chart).

=== ":material-note-edit: Examples"

	| :material-check: DO      | :material-close: DON'T  |
	| ----------- | --------- |
	| «`t/ha`» | «`t / ha`» |
	| «`%`» | «`pct`» |
	| «`kWh/person`» | «`pc`» |



---


#### `collection.metadata.sort`

*type*: `array`







---


#### `collection.metadata.sources`

*type*: `array`

List of all sources of the indicator. Automatically filled. NOTE: This is no longer in use, you should use origins.





---


#### `collection.metadata.title`

*type*: `string` | {==required==}

Title of the indicator, which is a few words definition of the indicator.

=== ":fontawesome-solid-list:  Guidelines"

	- Must start with a capital letter.
	- Must not end with a period.
	- Must be one short sentence (a few words).
	- For 'small datasets', this should be the publicly displayed title. For 'big datasets' (like FAOSTAT, with many dimensions), it can be less human-readable, optimized for internal searches (then, use `presentation.title_public` for the public title).
	- Should not mention other metadata fields like `producer` or `version`.

=== ":material-note-edit: Examples"

	| :material-check: DO      | :material-close: DON'T  |
	| ----------- | --------- |
	| «`Number of neutron star mergers in the Milky Way`» | «`Number of neutron stars (NASA)`» |
	| «`Share of neutron star mergers that happen in the Milky Way`» | «`Share of neutron star mergers that happen in the Milky Way (2023)`» |
	| «`Barley | 00000044 || Area harvested | 005312 || hectares`» | «`Barley`» |



---


#### `collection.metadata.type`

*type*: `string`

Indicator type is usually automatically inferred from the data, but must be manually set for ordinal and categorical types.





---


#### `collection.metadata.unit`

*type*: `string` | {==required==}

Very concise name of the unit we use to measure the indicator values.

=== ":fontawesome-solid-list:  Guidelines"

	- Must not start with a capital letter.
	- Must not end with a period.
	- Must be empty if the indicator has no units.
	- Must be in plural.
	- Must be a metric unit when applicable.
	- Should not use symbols like “/”.
		- If it is a derived unit, use 'per' to denote a division, e.g. '... per hectare', or '... per person'.
	- Should be '%' for percentages.

=== ":material-note-edit: Examples"

	| :material-check: DO      | :material-close: DON'T  |
	| ----------- | --------- |
	| «`tonnes per hectare`» | «`tonnes/hectare`» |
	| «`kilowatts per person`» | «`kilowatts per capita`» |



---


### `collection.title`

Title configuration for the collection display.

#### `collection.title.title`

*type*: `string` | {==required==}

Primary title displayed for the collection.

=== ":fontawesome-solid-list:  Guidelines"

	- Must start with a capital letter.
	- Must not end with a period.
	- Should be concise but descriptive.
	- Should clearly indicate the collection's purpose.

=== ":material-note-edit: Examples"

	| :material-check: DO      | :material-close: DON'T  |
	| ----------- | --------- |
	| «`Energy Mix`» | «`energy mix`», «`Energy Mix.`», «`Energy`» |
	| «`Global Population Demographics`» | «`global population demographics`», «`Demographics`» |
	| «`Climate Change Indicators`» | «`climate change indicators`», «`Climate`» |



---


#### `collection.title.title_variant`

*type*: `string` | {==required==}

Additional context or variant information for the title.

=== ":fontawesome-solid-list:  Guidelines"

	- Must not end with a period.
	- Should provide disambiguation or additional context.
	- Should be shorter than the main title.




---


### `collection.topic_tags`

*type*: `array` | optional

List of topic categories that this collection relates to.

=== ":fontawesome-solid-list:  Guidelines"

	- Must use existing OWID topic tags.
	- Should include 1-3 relevant topic tags.
	- The first tag should be the most relevant topic.

=== ":material-note-edit: Examples"

	| :material-check: DO      | :material-close: DON'T  |
	| ----------- | --------- |
	| «`["Energy"]`» | «`["energy"]`», «`["Invalid Topic"]`» |
	| «`["Climate Change", "Energy"]`» | «`["climate change", "energy"]`», «`["Too", "Many", "Tags"]`» |
	| «`["Population Growth", "Demography"]`» | «`["population growth", "demography"]`», «`["Nonexistent Topic"]`» |



---


### `collection.views`

*type*: `array` | {==required==}

Array of view configurations that define specific chart instances.

=== ":fontawesome-solid-list:  Guidelines"

	- Must contain at least one view.
	- Each view represents a specific combination of dimension choices and chart configuration.
	- Views are generated automatically based on dimension combinations, but can be customized.




---


## `view.config`

Chart configuration options that override default settings for this view.

### `view.config.$schema`

*type*: `string`

Url of the concrete schema version to use to validate this document





---


### `view.config.addCountryMode`

*type*: `string`

Whether the user can change countries, add additional ones or neither





---


### `view.config.baseColorScheme`

*type*: `string`

One of the predefined base color schemes.
If not provided, a default is automatically chosen based on the chart type.






---


### `view.config.chartTypes`

*type*: `array`

Which chart types should be shown





---


### `view.config.colorScale`

Color scale definition

#### `view.config.colorScale.baseColorScheme`

*type*: `string`

One of the predefined base color schemes.
If not provided, a default is automatically chosen based on the chart type.






---


#### `view.config.colorScale.binningStrategy`

*type*: `string`

The strategy for generating the bin boundaries





---


#### `view.config.colorScale.binningStrategyBinCount`

*type*: `integer`

The *suggested* number of bins for the automatic binning algorithm





---


#### `view.config.colorScale.colorSchemeInvert`

*type*: `boolean`

Reverse the order of colors in the color scheme





---


#### `view.config.colorScale.customCategoryColors`

Map of categorical values to colors. Colors are CSS colors, usually in the form `#aa9944`

#### `view.config.colorScale.customCategoryLabels`

Map of category values to color legend labels.

#### `view.config.colorScale.customHiddenCategories`

Allow hiding categories from the legend

#### `view.config.colorScale.customNumericColors`

*type*: `array`

Override some or all colors for the numerical color legend.
Colors are CSS colors, usually in the form `#aa9944`
`null` falls back the color scheme color.






---


#### `view.config.colorScale.customNumericColorsActive`

*type*: `boolean`

Whether `customNumericColors` are used to override the color scheme





---


#### `view.config.colorScale.customNumericLabels`

*type*: `array`

Custom labels for each numeric bin. Only applied when strategy is `manual`.
`null` falls back to default label.






---


#### `view.config.colorScale.customNumericMinValue`

*type*: `number`

The minimum bracket of the first bin. Inferred from data if not provided.





---


#### `view.config.colorScale.customNumericValues`

*type*: `array`

Custom maximum brackets for each numeric bin. Only applied when strategy is `manual`





---


#### `view.config.colorScale.legendDescription`

*type*: `string`

A custom legend description. Only used in ScatterPlot legend titles for now.





---


### `view.config.compareEndPointsOnly`

*type*: `boolean`

Drops in between points in scatter plots





---


### `view.config.comparisonLines`

*type*: `array`

List of vertical comparison lines to draw





---


### `view.config.entityType`

*type*: `string`

Display string for naming the primary entities of the data. Default is 'country or region', but you can specify a different one such as 'state' or 'region'





---


### `view.config.entityTypePlural`

*type*: `string`

Plural of the entity type (i.e. when entityType is 'country' this would be 'countries')





---


### `view.config.excludedEntityNames`

*type*: `array`

Entities that should be excluded (opposite of includedEntityNames)





---


### `view.config.facettingLabelByYVariables`

*type*: `string`

Display string that replaces 'metric' in the 'Split by metric' label in facet controls (e.g. 'product' displays 'Split by product')





---


### `view.config.focusedSeriesNames`

*type*: `array`

The initially focused chart elements. Is either a list of entity or variable names.
Only works for line and slope charts for now.






---


### `view.config.hasMapTab`

*type*: `boolean`

Indicates if the map tab should be shown





---


### `view.config.hideAnnotationFieldsInTitle`

Whether to hide any automatically added title annotations like the selected year

#### `view.config.hideAnnotationFieldsInTitle.changeInPrefix`

*type*: `boolean`

Whether to hide "Change in" in relative line charts





---


#### `view.config.hideAnnotationFieldsInTitle.entity`

*type*: `boolean`

Whether to hide the entity annotation





---


#### `view.config.hideAnnotationFieldsInTitle.time`

*type*: `boolean`

Whether to hide the time annotation





---


### `view.config.hideConnectedScatterLines`

*type*: `boolean`

Whether to hide connecting lines on scatter plots when a time range is selected





---


### `view.config.hideFacetControl`

*type*: `boolean`

Whether to hide the faceting control





---


### `view.config.hideLegend`

*type*: `boolean`

No description available.





---


### `view.config.hideLogo`

*type*: `boolean`

No description available.





---


### `view.config.hideRelativeToggle`

*type*: `boolean`

Whether to hide the relative mode UI toggle





---


### `view.config.hideScatterLabels`

*type*: `boolean`

Hide entity names in Scatter plots





---


### `view.config.hideTimeline`

*type*: `boolean`

Whether to hide the timeline from the user. If it is hidden then the user can't change the time





---


### `view.config.hideTotalValueLabel`

*type*: `boolean`

Whether to hide the total value label (used on stacked discrete bar charts)





---


### `view.config.includedEntityNames`

*type*: `array`

Entities that should be included (opposite of excludedEntityNames).
If empty, all available entities are used. If set, all entities not specified here are excluded.
excludedEntityNames are evaluated afterwards and can still remove entities even if they were included before.






---


### `view.config.internalNotes`

*type*: `string`

No description available.





---


### `view.config.invertColorScheme`

*type*: `boolean`

Reverse the order of colors in the color scheme





---


### `view.config.logo`

*type*: `string`

Which logo to show on the upper right side





---


### `view.config.map`

Configuration of the world map chart

#### `view.config.map.colorScale`

Color scale definition

##### `view.config.map.colorScale.baseColorScheme`

*type*: `string`

One of the predefined base color schemes.
If not provided, a default is automatically chosen based on the chart type.






---


##### `view.config.map.colorScale.binningStrategy`

*type*: `string`

The strategy for generating the bin boundaries





---


##### `view.config.map.colorScale.binningStrategyBinCount`

*type*: `integer`

The *suggested* number of bins for the automatic binning algorithm





---


##### `view.config.map.colorScale.colorSchemeInvert`

*type*: `boolean`

Reverse the order of colors in the color scheme





---


##### `view.config.map.colorScale.customCategoryColors`

Map of categorical values to colors. Colors are CSS colors, usually in the form `#aa9944`

##### `view.config.map.colorScale.customCategoryLabels`

Map of category values to color legend labels.

##### `view.config.map.colorScale.customHiddenCategories`

Allow hiding categories from the legend

##### `view.config.map.colorScale.customNumericColors`

*type*: `array`

Override some or all colors for the numerical color legend.
Colors are CSS colors, usually in the form `#aa9944`
`null` falls back the color scheme color.






---


##### `view.config.map.colorScale.customNumericColorsActive`

*type*: `boolean`

Whether `customNumericColors` are used to override the color scheme





---


##### `view.config.map.colorScale.customNumericLabels`

*type*: `array`

Custom labels for each numeric bin. Only applied when strategy is `manual`.
`null` falls back to default label.






---


##### `view.config.map.colorScale.customNumericMinValue`

*type*: `number`

The minimum bracket of the first bin. Inferred from data if not provided.





---


##### `view.config.map.colorScale.customNumericValues`

*type*: `array`

Custom maximum brackets for each numeric bin. Only applied when strategy is `manual`





---


##### `view.config.map.colorScale.legendDescription`

*type*: `string`

A custom legend description. Only used in ScatterPlot legend titles for now.





---


#### `view.config.map.columnSlug`

*type*: `string`

Column to show in the map tab. Can be a column slug (e.g. in explorers) or a variable ID (as string).
If not provided, the first y dimension is used.






---


#### `view.config.map.globe`

Configuration of the globe

##### `view.config.map.globe.isActive`

*type*: `boolean`

Whether the globe is initially shown





---


##### `view.config.map.globe.rotation`

*type*: `array`

Latitude and Longitude of the globe rotation





---


##### `view.config.map.globe.zoom`

*type*: `number`

Zoom level of the globe





---


#### `view.config.map.hideTimeline`

*type*: `boolean`

Whether the timeline should be hidden in the map view and thus the user not be able to change the year





---


#### `view.config.map.region`

*type*: `string`

Which region to focus on





---


#### `view.config.map.selectedEntityNames`

*type*: `array`

The initial selection of entities to show on the map





---


#### `view.config.map.time`

*type*: `number`, `string`

Select a specific time to be displayed.





---


#### `view.config.map.timeTolerance`

*type*: `integer`

Tolerance to use. If data points are missing for a time point, a match is accepted if it lies
within the specified time period. The unit is the dominating time unit, usually years but can be days for
daily time series. If not provided, the tolerance specified in the metadata of the indicator is used.
If that's not specified, 0 is used.






---


#### `view.config.map.toleranceStrategy`

*type*: `string`

Tolerance strategy to use. Options include accepting matches that are "closest" to the time point in question
(going forwards and backwards in time), and accepting matches that lie in the past ("backwards") or
the future ("forwards").






---


#### `view.config.map.tooltipUseCustomLabels`

*type*: `boolean`

Show the label from colorSchemeLabels in the tooltip instead of the numeric value





---


### `view.config.matchingEntitiesOnly`

*type*: `boolean`

Exclude entities that do not belong in any color group





---


### `view.config.maxTime`

*type*: `number`, `string`

End point of the initially selected time span.





---


### `view.config.minTime`

*type*: `number`, `string`

Start point of the initially selected time span.





---


### `view.config.missingDataStrategy`

*type*: `string`

The desired strategy for handling entities with missing data





---


### `view.config.note`

*type*: `string`

Note displayed in the footer of the chart. To be used for clarifications etc about the data.





---


### `view.config.originUrl`

*type*: `string`

The page containing this chart where more context can be found





---


### `view.config.relatedQuestions`

*type*: `array`

Links to related questions





---


### `view.config.scatterPointLabelStrategy`

*type*: `string`

When a user hovers over a connected series line in a ScatterPlot we show
a label for each point. By default that value will be from the "year" column
but by changing this option the column used for the x or y axis could be used instead.






---


### `view.config.selectedEntityColors`

Colors for selected entities

### `view.config.selectedEntityNames`

*type*: `array`

The initial selection of entities





---


### `view.config.selectedFacetStrategy`

*type*: `string`

The desired facetting strategy (none for no facetting)





---


### `view.config.showNoDataArea`

*type*: `boolean`

Whether to show an area for entities that have no data (currently only used in marimekko charts)





---


### `view.config.showYearLabels`

*type*: `boolean`

Whether to show year labels in bar charts





---


### `view.config.sortBy`

*type*: `string`

Sort criterium (used by stacked bar charts and marimekko)





---


### `view.config.sortColumnSlug`

*type*: `string`

Sort column if sortBy is column (used by stacked bar charts and marimekko)





---


### `view.config.sortOrder`

*type*: `string`

Sort order (used by stacked bar charts and marimekko)





---


### `view.config.sourceDesc`

*type*: `string`

Short comma-separated list of source names





---


### `view.config.stackMode`

*type*: `string`

Stack mode. Only absolute and relative are actively used.





---


### `view.config.subtitle`

*type*: `string`

The longer subtitle text to show beneath the title





---


### `view.config.tab`

*type*: `string`

The tab that is shown initially





---


### `view.config.timelineMaxTime`

*type*: `number`, `string`

The highest year to show in the timeline. If this is set then the user is not able to see
any data after this year. If set to "latest", then the latest year in the data is used.






---


### `view.config.timelineMinTime`

*type*: `number`, `string`

The lowest year to show in the timeline. If this is set then the user is not able to see
any data before this year. If set to "earliest", then the earliest year in the data is used.






---


### `view.config.title`

*type*: `string`

Big title text of the chart





---


### `view.config.variantName`

*type*: `string`

Optional internal variant name for distinguishing charts with the same title





---


### `view.config.version`

*type*: `integer`

No description available.





---


### `view.config.xAxis`



#### `view.config.xAxis.canChangeScaleType`

*type*: `boolean`

Allow user to change lin/log





---


#### `view.config.xAxis.facetDomain`

*type*: `string`

Whether the axis domain should be the same across faceted charts (if possible)





---


#### `view.config.xAxis.label`

*type*: `string`

Axis label





---


#### `view.config.xAxis.max`

*type*: `number`, `string`

Maximum domain value of the axis. Inferred from data if set to "auto".





---


#### `view.config.xAxis.min`

*type*: `number`, `string`

Minimum domain value of the axis. Inferred from data if set to "auto".
Usually defaults to "auto", but defaults to 0 for line charts on the y-axis.






---


#### `view.config.xAxis.removePointsOutsideDomain`

*type*: `boolean`

No description available.





---


#### `view.config.xAxis.scaleType`

*type*: `string`

Toggle linear/logarithmic





---


### `view.config.yAxis`



#### `view.config.yAxis.canChangeScaleType`

*type*: `boolean`

Allow user to change lin/log





---


#### `view.config.yAxis.facetDomain`

*type*: `string`

Whether the axis domain should be the same across faceted charts (if possible)





---


#### `view.config.yAxis.label`

*type*: `string`

Axis label





---


#### `view.config.yAxis.max`

*type*: `number`, `string`

Maximum domain value of the axis. Inferred from data if set to "auto".





---


#### `view.config.yAxis.min`

*type*: `number`, `string`

Minimum domain value of the axis. Inferred from data if set to "auto".
Usually defaults to "auto", but defaults to 0 for line charts on the y-axis.






---


#### `view.config.yAxis.removePointsOutsideDomain`

*type*: `boolean`

No description available.





---


#### `view.config.yAxis.scaleType`

*type*: `string`

Toggle linear/logarithmic





---


### `view.config.zoomToSelection`

*type*: `boolean`

Whether to zoom to the selected data points





---



For the complete list of available configuration options, see the [Grapher schema](https://files.ourworldindata.org/schemas/grapher-schema.008.json).


## `view.metadata`



### `view.metadata.description_from_producer`

*type*: `string` | recommended (if existing)

Description of the indicator written by the producer, if any was given.

=== ":fontawesome-solid-list:  Guidelines"

	- Must start with a capital letter.
	- Must end with a period.
	- Should be identical to the producer's text, except for some formatting changes, typo corrections, or other appropriate minor edits.
	- Should only be given if the producer clearly provides such definitions in a structured way. Avoid spending time searching for a definition given by the producer elsewhere.




---


### `view.metadata.description_key`

*type*: `array` | recommended (for curated indicators)

List of key pieces of information about the indicator.

=== ":fontawesome-solid-list:  Guidelines"

	- Must be a list of one or more short paragraphs.
		- Each paragraph must start with a capital letter.
		- Each paragraph must end with a period.
	- Must not contain `description_short` (although there might be some overlap of information).
	- Should contain all the key information about the indicator (except that already given in `description_short`).
	- Should include the key information given in other fields like `grapher_config.subtitle` (if different from `description_short`) and `grapher_config.note`.
	- Should not contain information about processing (which should be in `description_processing`).
	- Should only contain information that is key to the public.
		- Anything that is too detailed or technical should be left in the code.




---


### `view.metadata.description_processing`

*type*: `string` | {==required (if applicable)==}

Relevant information about the processing of the indicator done by OWID.

=== ":fontawesome-solid-list:  Guidelines"

	- Must start with a capital letter.
	- Must end with a period.
	- Must be used if important editorial decisions have been taken during data processing.
	- Must not be used to describe common processing steps like country harmonization.
	- Should only contain key processing information to the public.
		- Anything that is too detailed or technical should be left in the code.




---


### `view.metadata.description_short`

*type*: `string` | {==required==}

One or a few lines that complement the title to have a short description of the indicator.

=== ":fontawesome-solid-list:  Guidelines"

	- Must start with a capital letter.
	- Must end with a period.
	- Must be one short paragraph (for example suitable to fit in a chart subtitle).
	- Should not mention any other metadata fields (like information about the processing, or the origins, or the units). **Exceptions:**
		- The unit can be mentioned if it is crucial for the description.




---


### `view.metadata.display`

We keep display for the time being as the 'less powerful sibling' of grapher config.

#### `view.metadata.display.color`

*type*: `string`

Color to use for the indicator in e.g. line charts.





---


#### `view.metadata.display.conversionFactor`

*type*: `number`

Conversion factor to apply to indicator values.

=== ":fontawesome-solid-list:  Guidelines"

	- **Note:** We should avoid using this, and instead convert data and units (and possibly other metadata fields where the units are mentioned) consistently in the ETL grapher step.




---


#### `view.metadata.display.description`

*type*: `string`

Description to display for the indicator, to replace the indicator's `description`.





---


#### `view.metadata.display.entityAnnotationsMap`

*type*: `string`

Entity annotations





---


#### `view.metadata.display.includeInTable`

*type*: `boolean`

Whether to render this indicator in the table sheet.





---


#### `view.metadata.display.isProjection`

*type*: `boolean`, `string`, `string`

Indicates if this time series is a forward projection (if so then this is rendered differently in e.g. line charts).





---


#### `view.metadata.display.name`

*type*: `string` | {==required==}

Indicator's title to display in the legend of a chart. NOTE: For backwards compatibility, `display.name` also replaces the indicator's title in other public places. Therefore, whenever `display.name` is defined, `title_public` should also be defined.

=== ":fontawesome-solid-list:  Guidelines"

	- Must be very short, to fit the legend of a chart.
	- Must not end with a period.
	- Should not mention other metadata fields like `producer` or `version`.

=== ":material-note-edit: Examples"

	| :material-check: DO      | :material-close: DON'T  |
	| ----------- | --------- |
	| «`Agriculture`» | «`Nitrous oxide emissions from agriculture`» |
	| «`Area harvested`» | «`Barley | 00000044 || Area harvested | 005312 || hectares`» |



---


#### `view.metadata.display.numDecimalPlaces`

*type*: `integer`, `string`, `string`

Number of decimal places to show in charts (and in the table tab).





---


#### `view.metadata.display.numSignificantFigures`

*type*: `integer`, `string`

Number of significant rounding figures in charts.





---


#### `view.metadata.display.roundingMode`

*type*: `string`

Specifies the rounding mode to use.





---


#### `view.metadata.display.shortUnit`

*type*: `string`

Short unit to use in charts instead of the indicator's `short_unit`.





---


#### `view.metadata.display.tableDisplay`

Configuration for the table tab for this indicator, with options `hideAbsoluteChange` and `hideRelativeChange`.

##### `view.metadata.display.tableDisplay.hideAbsoluteChange`

*type*: `boolean`

Whether to hide the absolute change.





---


##### `view.metadata.display.tableDisplay.hideRelativeChange`

*type*: `boolean`

Whether to hide the relative change.





---


#### `view.metadata.display.tolerance`

*type*: `integer`

Tolerance (in years or days) to use in charts. If data points are missing, the closest data point will be shown, if it lies within the specified tolerance.





---


#### `view.metadata.display.unit`

*type*: `string`

Unit to use in charts instead of the indicator's `unit`.





---


#### `view.metadata.display.yearIsDay`

*type*: `boolean`

Switch to indicate if the number in the year column represents a day (since zeroDay) or a year.





---


#### `view.metadata.display.zeroDay`

*type*: `string`

ISO date day string for the starting date if `yearIsDay` is `True`.





---


### `view.metadata.license`

*type*: `string` | {==required (in the future this could be automatic)==}

License of the indicator, which depends on the indicator's processing level and the origins' licenses.

=== ":fontawesome-solid-list:  Guidelines"

	- If the indicator's `processing_level` is major, assign `CC BY 4.0`.
	- If the indicator's `processing_level` is minor, choose the most strict license among the origins' `licenses`.




---


### `view.metadata.origins`

*type*: `array`

List of all origins of the indicator.

=== ":fontawesome-solid-list:  Guidelines"

	- **Note:** Origins should be propagated automatically from snapshots. Therefore, this field should only be manually filled out if automatic propagation fails.




---


### `view.metadata.presentation`

An indicator's presentation defines how the indicator's metadata will be shown on our website (e.g. in data pages). The indicator presentation metadata fields are the attributes of the `VariablePresentationMeta`object in ETL.

#### `view.metadata.presentation.attribution`

*type*: `string` | optional

Citation of the indicator's origins, to override the automatic format `producer1 (year1); producer2 (year2)`.

=== ":fontawesome-solid-list:  Guidelines"

	- Must start with a capital letter. **Exceptions:**
		- The name of the institution or the author must be spelled with small letter, e.g. `van Haasteren`.
	- Must join multiple attributions by a `;`.
	- Must not end in a period (and must **not** end in `;`).
	- Must contain the year of `date_published`, for each origin, in parenthesis.
	- Should only be used when the automatic format `producer1 (year1); producer2 (year2)` needs to be overridden.

=== ":material-note-edit: Examples"

	| :material-check: DO      | :material-close: DON'T  |
	| ----------- | --------- |
	| «`Energy Institute - Statistical Review of World Energy (2023); Ember (2022)`» | «`UN (2023), WHO (2023)`» |



---


#### `view.metadata.presentation.attribution_short`

*type*: `string` | recommended (for curated indicators)

Very short citation of the indicator's main producer(s).

=== ":fontawesome-solid-list:  Guidelines"

	- Must start with a capital letter. **Exceptions:**
		- The name of the institution or the author must be spelled with small letter, e.g. `van Haasteren`.
	- Must not end in a period.
	- Should be very short.
	- Should be used if the automatic concatenation of origin's `attribution_short` are too long. In those cases, choose the most important `attribution` (e.g. the main producer of the data).




---


#### `view.metadata.presentation.faqs`

*type*: `array` | recommended (for curated indicators)

List of references to questions in an FAQ google document, relevant to the indicator.

=== ":fontawesome-solid-list:  Guidelines"

	- Each reference must contain `fragment_id` (question identifier) and `gdoc_id` (document identifier).




---


#### `view.metadata.presentation.grapher_config`

Our World in Data grapher configuration. Most of the fields can be left empty and will be filled with reasonable default values.

Find more details on its attributes [here](https://files.ourworldindata.org/schemas/grapher-schema.003.json).

##### `view.metadata.presentation.grapher_config.$schema`

*type*: `string`

Url of the concrete schema version to use to validate this document





---


##### `view.metadata.presentation.grapher_config.addCountryMode`

*type*: `string`

Whether the user can change countries, add additional ones or neither





---


##### `view.metadata.presentation.grapher_config.baseColorScheme`

*type*: `string`

The default color scheme if no color overrides are specified





---


##### `view.metadata.presentation.grapher_config.chartTypes`

*type*: `array`

Which types of chart should be shown





---


##### `view.metadata.presentation.grapher_config.colorScale`

Color scale definition

###### `view.metadata.presentation.grapher_config.colorScale.baseColorScheme`

*type*: `['YlGn', 'YlGnBu', 'GnBu', 'BuGn', 'PuBuGn', 'BuPu', 'RdPu', 'PuRd', 'OrRd', 'YlOrRd', 'YlOrBr', 'Purples', 'Blues', 'Greens', 'Oranges', 'Reds', 'Greys', 'PuOr', 'BrBG', 'PRGn', 'PiYG', 'RdBu', 'RdGy', 'RdYlBu', 'Spectral', 'RdYlGn', 'Accent', 'Dark2', 'Paired', 'Pastel1', 'Pastel2', 'Set1', 'Set2', 'Set3', 'PuBu', 'hsv-RdBu', 'hsv-CyMg', 'Magma', 'Inferno', 'Plasma', 'Viridis', 'continents', 'stackedAreaDefault', 'owid-distinct', 'default', 'ContinentsLines', 'SingleColorDenim', 'SingleColorTeal', 'SingleColorPurple', 'SingleColorDustyCoral', 'SingleColorDarkCopper', 'OwidCategoricalA', 'OwidCategoricalB', 'OwidCategoricalC', 'OwidCategoricalD', 'OwidCategoricalE', 'OwidEnergy', 'OwidEnergyLines', 'OwidDistinctLines', 'BinaryMapPaletteA', 'BinaryMapPaletteB', 'BinaryMapPaletteC', 'BinaryMapPaletteD', 'BinaryMapPaletteE', 'SingleColorGradientDenim', 'SingleColorGradientTeal', 'SingleColorGradientPurple', 'SingleColorGradientDustyCoral', 'SingleColorGradientDarkCopper']`, `string`, `string`, `array`

One of the predefined base color schemes





---


###### `view.metadata.presentation.grapher_config.colorScale.binningStrategy`

*type*: `string`

The strategy for generating the bin boundaries





---


###### `view.metadata.presentation.grapher_config.colorScale.binningStrategyBinCount`

*type*: `integer`

The *suggested* number of bins for the automatic binning algorithm





---


###### `view.metadata.presentation.grapher_config.colorScale.colorSchemeInvert`

*type*: `boolean`

Reverse the order of colors in the color scheme





---


###### `view.metadata.presentation.grapher_config.colorScale.customCategoryColors`

Map of categorical values to colors. Colors are CSS colors, usually in the form `#aa9944`

###### `view.metadata.presentation.grapher_config.colorScale.customCategoryLabels`

Map of category values to color legend labels.

###### `view.metadata.presentation.grapher_config.colorScale.customHiddenCategories`

Allow hiding categories from the legend

###### `view.metadata.presentation.grapher_config.colorScale.customNumericColors`

*type*: `array`

Override some or all colors for the numerical color legend.
Colors are CSS colors, usually in the form `#aa9944`
`null` falls back the color scheme color.






---


###### `view.metadata.presentation.grapher_config.colorScale.customNumericColorsActive`

*type*: `boolean`

Whether `customNumericColors` are used to override the color scheme





---


###### `view.metadata.presentation.grapher_config.colorScale.customNumericLabels`

*type*: `array`

Custom labels for each numeric bin. Only applied when strategy is `manual`.
`null` falls back to default label.






---


###### `view.metadata.presentation.grapher_config.colorScale.customNumericMinValue`

*type*: `number`, `string`

The minimum bracket of the first bin





---


###### `view.metadata.presentation.grapher_config.colorScale.customNumericValues`

*type*: `array`, `string`

Custom maximum brackets for each numeric bin. Only applied when strategy is `manual` or when using a string matching pattern like `<%`.





---


###### `view.metadata.presentation.grapher_config.colorScale.legendDescription`

*type*: `string`

A custom legend description. Only used in ScatterPlot legend titles for now.





---


##### `view.metadata.presentation.grapher_config.compareEndPointsOnly`

*type*: `boolean`

Drops in between points in scatter plots





---


##### `view.metadata.presentation.grapher_config.comparisonLines`

*type*: `array`

List of vertical comparison lines to draw





---


##### `view.metadata.presentation.grapher_config.data`

Obsolete name - used only to store the available entities

###### `view.metadata.presentation.grapher_config.data.availableEntities`

*type*: `array`

List of available entities





---


##### `view.metadata.presentation.grapher_config.dimensions`

*type*: `array`

List of dimensions and their mapping to variables





---


##### `view.metadata.presentation.grapher_config.entityType`

*type*: `string`

Display string for naming the primary entities of the data. Default is 'country or region', but you can specify a different one such as 'state' or 'region'





---


##### `view.metadata.presentation.grapher_config.entityTypePlural`

*type*: `string`

Plural of the entity type (i.e. when entityType is 'country' this would be 'countries')





---


##### `view.metadata.presentation.grapher_config.excludedEntityNames`

*type*: `array`

Entity names that should be excluded from the chart





---


##### `view.metadata.presentation.grapher_config.facettingLabelByYVariables`

*type*: `string`

Display string that replaces 'metric' in the 'Split by metric' label in facet controls (e.g. 'product' displays 'Split by product')





---


##### `view.metadata.presentation.grapher_config.hasMapTab`

*type*: `boolean`, `string`

Whether the default chart for the indicator should include a map tab.





---


##### `view.metadata.presentation.grapher_config.hideAnnotationFieldsInTitle`

Whether to hide any automatically added title annotations like the selected year

###### `view.metadata.presentation.grapher_config.hideAnnotationFieldsInTitle.changeInPrefix`

*type*: `boolean`

Whether to hide "Change in" in relative line charts





---


###### `view.metadata.presentation.grapher_config.hideAnnotationFieldsInTitle.entity`

*type*: `boolean`

Whether to hide the entity annotation





---


###### `view.metadata.presentation.grapher_config.hideAnnotationFieldsInTitle.time`

*type*: `boolean`

Whether to hide the time annotation





---


##### `view.metadata.presentation.grapher_config.hideConnectedScatterLines`

*type*: `boolean`

Whether to hide connecting lines on scatter plots when a time range is selected





---


##### `view.metadata.presentation.grapher_config.hideFacetControl`

*type*: `boolean`

Whether to hide the faceting control





---


##### `view.metadata.presentation.grapher_config.hideLegend`

*type*: `boolean`

Hide legend in chart.





---


##### `view.metadata.presentation.grapher_config.hideLogo`

*type*: `boolean`

Hide logo in chart.





---


##### `view.metadata.presentation.grapher_config.hideRelativeToggle`

*type*: `boolean`

Whether to hide the relative mode UI toggle. Default depends on the chart type





---


##### `view.metadata.presentation.grapher_config.hideScatterLabels`

*type*: `boolean`

Hide entity names in Scatter plots





---


##### `view.metadata.presentation.grapher_config.hideTimeline`

*type*: `boolean`

Whether to hide the timeline from the user. If it is hidden then the user can't change the time





---


##### `view.metadata.presentation.grapher_config.hideTotalValueLabel`

*type*: `boolean`

Whether to hide the total value label (used on stacked discrete bar charts)





---


##### `view.metadata.presentation.grapher_config.id`

*type*: `integer`

Internal DB id. Useful internally for OWID but not required if just using grapher directly.





---


##### `view.metadata.presentation.grapher_config.includedEntities`

*type*: `array`

Entity names to include. Opposite of includedEntityNames. If this is set then all entities not specified here are excluded.





---


##### `view.metadata.presentation.grapher_config.internalNotes`

*type*: `string`

Internal notes.





---


##### `view.metadata.presentation.grapher_config.invertColorScheme`

*type*: `boolean`

Reverse the order of colors in the color scheme





---


##### `view.metadata.presentation.grapher_config.isPublished`

*type*: `boolean`

Indicates if the chart is published on Our World in Data or still in draft





---


##### `view.metadata.presentation.grapher_config.logo`

*type*: `string`

Which logo to show on the upper right side





---


##### `view.metadata.presentation.grapher_config.map`

Configuration of the world map chart

###### `view.metadata.presentation.grapher_config.map.colorScale`

Color scale definition

####### `view.metadata.presentation.grapher_config.map.colorScale.baseColorScheme`

*type*: `['YlGn', 'YlGnBu', 'GnBu', 'BuGn', 'PuBuGn', 'BuPu', 'RdPu', 'PuRd', 'OrRd', 'YlOrRd', 'YlOrBr', 'Purples', 'Blues', 'Greens', 'Oranges', 'Reds', 'Greys', 'PuOr', 'BrBG', 'PRGn', 'PiYG', 'RdBu', 'RdGy', 'RdYlBu', 'Spectral', 'RdYlGn', 'Accent', 'Dark2', 'Paired', 'Pastel1', 'Pastel2', 'Set1', 'Set2', 'Set3', 'PuBu', 'hsv-RdBu', 'hsv-CyMg', 'Magma', 'Inferno', 'Plasma', 'Viridis', 'continents', 'stackedAreaDefault', 'owid-distinct', 'default', 'ContinentsLines', 'SingleColorDenim', 'SingleColorTeal', 'SingleColorPurple', 'SingleColorDustyCoral', 'SingleColorDarkCopper', 'OwidCategoricalA', 'OwidCategoricalB', 'OwidCategoricalC', 'OwidCategoricalD', 'OwidCategoricalE', 'OwidEnergy', 'OwidEnergyLines', 'OwidDistinctLines', 'BinaryMapPaletteA', 'BinaryMapPaletteB', 'BinaryMapPaletteC', 'BinaryMapPaletteD', 'BinaryMapPaletteE', 'SingleColorGradientDenim', 'SingleColorGradientTeal', 'SingleColorGradientPurple', 'SingleColorGradientDustyCoral', 'SingleColorGradientDarkCopper']`, `string`, `string`, `array`

One of the predefined base color schemes





---


####### `view.metadata.presentation.grapher_config.map.colorScale.binningStrategy`

*type*: `string`

The strategy for generating the bin boundaries





---


####### `view.metadata.presentation.grapher_config.map.colorScale.binningStrategyBinCount`

*type*: `integer`

The *suggested* number of bins for the automatic binning algorithm





---


####### `view.metadata.presentation.grapher_config.map.colorScale.colorSchemeInvert`

*type*: `boolean`

Reverse the order of colors in the color scheme





---


####### `view.metadata.presentation.grapher_config.map.colorScale.customCategoryColors`

Map of categorical values to colors. Colors are CSS colors, usually in the form `#aa9944`

####### `view.metadata.presentation.grapher_config.map.colorScale.customCategoryLabels`

Map of category values to color legend labels.

####### `view.metadata.presentation.grapher_config.map.colorScale.customHiddenCategories`

Allow hiding categories from the legend

####### `view.metadata.presentation.grapher_config.map.colorScale.customNumericColors`

*type*: `array`

Override some or all colors for the numerical color legend.
Colors are CSS colors, usually in the form `#aa9944`
`null` falls back the color scheme color.






---


####### `view.metadata.presentation.grapher_config.map.colorScale.customNumericColorsActive`

*type*: `boolean`

Whether `customNumericColors` are used to override the color scheme





---


####### `view.metadata.presentation.grapher_config.map.colorScale.customNumericLabels`

*type*: `array`

Custom labels for each numeric bin. Only applied when strategy is `manual`.
`null` falls back to default label.






---


####### `view.metadata.presentation.grapher_config.map.colorScale.customNumericMinValue`

*type*: `number`, `string`

The minimum bracket of the first bin





---


####### `view.metadata.presentation.grapher_config.map.colorScale.customNumericValues`

*type*: `array`, `string`

Custom maximum brackets for each numeric bin. Only applied when strategy is `manual` or when using a string matching pattern like `<%`.





---


####### `view.metadata.presentation.grapher_config.map.colorScale.legendDescription`

*type*: `string`

A custom legend description. Only used in ScatterPlot legend titles for now.





---


###### `view.metadata.presentation.grapher_config.map.hideTimeline`

*type*: `boolean`

Whether the timeline should be hidden in the map view and thus the user not be able to change the year





---


###### `view.metadata.presentation.grapher_config.map.projection`

*type*: `string`

Slightly misnamed - this does not change the map projection but instead specifies which world area to focus on.





---


###### `view.metadata.presentation.grapher_config.map.time`

*type*: `number`, `string`

Select a specific time to be displayed.





---


###### `view.metadata.presentation.grapher_config.map.timeTolerance`

*type*: `integer`

Tolerance to use. If data points are missing for a time point, a match is accepted if it lies
within the specified time period. The unit is the dominating time unit, usually years but can be days for
daily time series.






---


###### `view.metadata.presentation.grapher_config.map.toleranceStrategy`

*type*: `string`

Tolerance strategy to use. Options include accepting matches that are "closest" to the time point in question
(going forwards and backwards in time), and accepting matches that lie in the past ("backwards") or
the future ("forwards").






---


###### `view.metadata.presentation.grapher_config.map.tooltipUseCustomLabels`

*type*: `boolean`

Show the label from colorSchemeLabels in the tooltip instead of the numeric value





---


###### `view.metadata.presentation.grapher_config.map.variableId`

*type*: `integer`

Variable ID to show. TODO: remove this and use dimensions instead





---


##### `view.metadata.presentation.grapher_config.matchingEntitiesOnly`

*type*: `boolean`

Exclude entities that do not belong in any color group





---


##### `view.metadata.presentation.grapher_config.maxTime`

*type*: `number`, `string`

End point of the initially selected time span.





---


##### `view.metadata.presentation.grapher_config.minTime`

*type*: `number`, `string`

Start point of the initially selected time span.





---


##### `view.metadata.presentation.grapher_config.missingDataStrategy`

*type*: `string`

The desired strategy for handling entities with missing data





---


##### `view.metadata.presentation.grapher_config.note`

*type*: `string`

Default footer note to use in charts for the indicator.

=== ":fontawesome-solid-list:  Guidelines"

	- Must start with a capital letter.
	- Must end with a period.
	- Must not include 'Note:' at the beginning (this is added automatically).
	- Must be one short paragraph, usually one sentence, suitable to fit in a chart footer note.
	- Should not mention any other metadata fields.




---


##### `view.metadata.presentation.grapher_config.originUrl`

*type*: `string`

The page containing this chart where more context can be found





---


##### `view.metadata.presentation.grapher_config.relatedQuestions`

*type*: `array`

Links to related questions





---


##### `view.metadata.presentation.grapher_config.scatterPointLabelStrategy`

*type*: `string`

When a user hovers over a connected series line in a ScatterPlot we show
a label for each point. By default that value will be from the "year" column
but by changing this option the column used for the x or y axis could be used instead.






---


##### `view.metadata.presentation.grapher_config.selectedEntityColors`

Colors for selected entities

##### `view.metadata.presentation.grapher_config.selectedEntityNames`

*type*: `array`

List of entities (e.g. harmonized country names) to use by default in charts for the indicator.





---


##### `view.metadata.presentation.grapher_config.selectedFacetStrategy`

*type*: `string`

The desired facetting strategy (none for no facetting)





---


##### `view.metadata.presentation.grapher_config.showNoDataArea`

*type*: `boolean`

Whether to show an area for entities that have no data (currently only used in marimekko charts)





---


##### `view.metadata.presentation.grapher_config.showYearLabels`

*type*: `boolean`

Whether to show year labels in bar charts





---


##### `view.metadata.presentation.grapher_config.sortBy`

*type*: `string`

Sort criterium (used by stacked bar charts and marimekko)





---


##### `view.metadata.presentation.grapher_config.sortColumnSlug`

*type*: `string`

Sort column if sortBy is column (used by stacked bar charts and marimekko)





---


##### `view.metadata.presentation.grapher_config.sortOrder`

*type*: `string`

Sort order (used by stacked bar charts and marimekko)





---


##### `view.metadata.presentation.grapher_config.sourceDesc`

*type*: `string`

Short comma-separated list of source names





---


##### `view.metadata.presentation.grapher_config.stackMode`

*type*: `string`, `null`

Stack mode. Only absolute and relative are actively used.





---


##### `view.metadata.presentation.grapher_config.subtitle`

*type*: `string`

Default subtitle to use in charts for the indicator. NOTE: Use this field to override an indicator's `description_short` with information that is more suitable for a chart subtitle.

=== ":fontawesome-solid-list:  Guidelines"

	- Must start with a capital letter.
	- Must end with a period.
	- Must be one short paragraph, suitable to fit in a chart subtitle.




---


##### `view.metadata.presentation.grapher_config.tab`

*type*: `['chart', 'map', 'table']`, `string`

The tab that is shown initially





---


##### `view.metadata.presentation.grapher_config.timelineMaxTime`

*type*: `integer`

The highest year to show in the timeline. If this is set then the user is not able to see
any data after this year






---


##### `view.metadata.presentation.grapher_config.timelineMinTime`

*type*: `integer`

The lowest year to show in the timeline. If this is set then the user is not able to see
any data before this year






---


##### `view.metadata.presentation.grapher_config.title`

*type*: `string`

Default title to use in charts for the indicator, overriding the indicator's `title`.

=== ":fontawesome-solid-list:  Guidelines"

	- Must start with a capital letter.
	- Must not end with a period.
	- Must be one short sentence (a few words).
	- Must fit and be an appropriate choice for a chart's public title.
	- Should not mention other metadata fields like `producer` or `version`.




---


##### `view.metadata.presentation.grapher_config.variantName`

*type*: `string`

Optional internal variant name for distinguishing charts with the same title





---


##### `view.metadata.presentation.grapher_config.version`

*type*: `integer`







---


##### `view.metadata.presentation.grapher_config.xAxis`

Axis definition

###### `view.metadata.presentation.grapher_config.xAxis.canChangeScaleType`

*type*: `boolean`

Allow user to change lin/log





---


###### `view.metadata.presentation.grapher_config.xAxis.facetDomain`

*type*: `string`

Whether the axis domain should be the same across faceted charts (if possible)





---


###### `view.metadata.presentation.grapher_config.xAxis.label`

*type*: `string`

Axis label





---


###### `view.metadata.presentation.grapher_config.xAxis.max`

*type*: `number`

Maximum domain value of the axis





---


###### `view.metadata.presentation.grapher_config.xAxis.min`

*type*: `number`

Minimum domain value of the axis





---


###### `view.metadata.presentation.grapher_config.xAxis.removePointsOutsideDomain`

*type*: `boolean`







---


###### `view.metadata.presentation.grapher_config.xAxis.scaleType`

*type*: `string`

Toggle linear/logarithmic





---


##### `view.metadata.presentation.grapher_config.yAxis`

Axis definition

###### `view.metadata.presentation.grapher_config.yAxis.canChangeScaleType`

*type*: `boolean`

Allow user to change lin/log





---


###### `view.metadata.presentation.grapher_config.yAxis.facetDomain`

*type*: `string`

Whether the axis domain should be the same across faceted charts (if possible)





---


###### `view.metadata.presentation.grapher_config.yAxis.label`

*type*: `string`

Axis label





---


###### `view.metadata.presentation.grapher_config.yAxis.max`

*type*: `number`

Maximum domain value of the axis





---


###### `view.metadata.presentation.grapher_config.yAxis.min`

*type*: `number`

Minimum domain value of the axis





---


###### `view.metadata.presentation.grapher_config.yAxis.removePointsOutsideDomain`

*type*: `boolean`







---


###### `view.metadata.presentation.grapher_config.yAxis.scaleType`

*type*: `string`

Toggle linear/logarithmic





---


##### `view.metadata.presentation.grapher_config.zoomToSelection`

*type*: `boolean`

Whether to zoom to the selected data points





---


#### `view.metadata.presentation.title_public`

*type*: `string` | optional

Indicator title to be shown in public places like data pages, that overrides the indicator's title.

=== ":fontawesome-solid-list:  Guidelines"

	- Must start with a capital letter.
	- Must not end with a period.
	- Must be one short sentence (a few words).
	- Should not mention other metadata fields like `producer` or `version`.
	- Should help OWID and expert users identify the indicator.
	- For big datasets where constructing human-readable titles is hard (e.g. FAOSTAT), this field is mandatory, to improve the public appearance of the title.
	- When `display.name` is defined (to edit the default title in the legend of a chart), `title_public` must also be defined (otherwise `display.name` will be used as a public title).




---


#### `view.metadata.presentation.title_variant`

*type*: `string` | optional

Short disambiguation of the title that references a special feature of the methods or nature of the data.

=== ":fontawesome-solid-list:  Guidelines"

	- Must start with a capital letter.
	- Must not end in a period.
	- Should be very short.
	- Should only be used if the indicator's title is ambiguous (e.g. if there are multiple indicators with the same title).
	- Should not reference the data provider. For that, instead, use `attribution_short`.

=== ":material-note-edit: Examples"

	| :material-check: DO      | :material-close: DON'T  |
	| ----------- | --------- |
	| «`Age-standardized`» | «`The data is age-standardized`» |
	| «`Historical data`» | «`Historical data from 1800 to 2010`» |



---


#### `view.metadata.presentation.topic_tags`

*type*: `array` | recommended (for curated indicators)

List of topics where the indicator is relevant.

=== ":fontawesome-solid-list:  Guidelines"

	- Must be an existing topic tag, and be spelled correctly (see the list of topic tags in [datasette](http://analytics/private?sql=SELECT%0D%0A++DISTINCT+t.name%0D%0AFROM%0D%0A++tag_graph+tg%0D%0A++LEFT+JOIN+tags+t+ON+tg.childId+%3D+t.id%0D%0A++LEFT+JOIN+posts_gdocs+p+ON+t.slug+%3D+p.slug%0D%0A++AND+p.published+%3D+1%0D%0A++AND+p.type+IN+%28%27article%27%2C+%27topic-page%27%2C+%27linear-topic-page%27%29%0D%0AWHERE%0D%0A++p.slug+IS+NOT+NULL%0D%0AUNION%0D%0ASELECT%0D%0A++%27Uncategorized%27%0D%0AORDER+BY%0D%0A++t.name) (requires Tailscale).
	- The first tag must correspond to the most relevant topic page (since that topic page will be used in citations of this indicator).
	- Should contain 1, 2, or at most 3 tags.




---


### `view.metadata.presentation_license`

License to display for the indicator, overriding `license`.

#### `view.metadata.presentation_license.name`

*type*: `string`







---


#### `view.metadata.presentation_license.url`

*type*: `string`







---


### `view.metadata.processing_level`

*type*: `['minor', 'major']`, `string` | {==required (in the future this could be automatic).==}

Level of processing that the indicator values have experienced.

=== ":fontawesome-solid-list:  Guidelines"

	- Must be `minor` if the indicator has undergone only minor operations since its origin:
		- Rename entities (e.g. countries or columns)
		- Multiplication by a constant (e.g. unit change)
		- Drop missing values.
	- Must be `major` if any other operation is used:
		- Data aggregates (e.g. sum data for continents or income groups)
		- Operations between indicators (e.g. per capita, percentages, annual changes)
		- Concatenation of indicators, etc.




---


### `view.metadata.short_unit`

*type*: `string` | {==required==}

Characters that represent the unit we use to measure the indicator value.

=== ":fontawesome-solid-list:  Guidelines"

	- Must follow the rules of capitalization of the International System of Units, when applicable.
	- Must not end with a period.
	- Must be empty if the indicator has no units.
	- Should not contain spaces.
	- If, for clarity, we prefer to simplify the units in a chart, e.g. to show `kWh` instead of `kWh/person`, use `display.short_unit` for the simplified units, and keep the correct one in `indicator.short_unit` (and ensure there is no ambiguity in the chart).

=== ":material-note-edit: Examples"

	| :material-check: DO      | :material-close: DON'T  |
	| ----------- | --------- |
	| «`t/ha`» | «`t / ha`» |
	| «`%`» | «`pct`» |
	| «`kWh/person`» | «`pc`» |



---


### `view.metadata.sort`

*type*: `array`







---


### `view.metadata.sources`

*type*: `array`

List of all sources of the indicator. Automatically filled. NOTE: This is no longer in use, you should use origins.





---


### `view.metadata.title`

*type*: `string` | {==required==}

Title of the indicator, which is a few words definition of the indicator.

=== ":fontawesome-solid-list:  Guidelines"

	- Must start with a capital letter.
	- Must not end with a period.
	- Must be one short sentence (a few words).
	- For 'small datasets', this should be the publicly displayed title. For 'big datasets' (like FAOSTAT, with many dimensions), it can be less human-readable, optimized for internal searches (then, use `presentation.title_public` for the public title).
	- Should not mention other metadata fields like `producer` or `version`.

=== ":material-note-edit: Examples"

	| :material-check: DO      | :material-close: DON'T  |
	| ----------- | --------- |
	| «`Number of neutron star mergers in the Milky Way`» | «`Number of neutron stars (NASA)`» |
	| «`Share of neutron star mergers that happen in the Milky Way`» | «`Share of neutron star mergers that happen in the Milky Way (2023)`» |
	| «`Barley | 00000044 || Area harvested | 005312 || hectares`» | «`Barley`» |



---


### `view.metadata.type`

*type*: `string`

Indicator type is usually automatically inferred from the data, but must be manually set for ordinal and categorical types.





---


### `view.metadata.unit`

*type*: `string` | {==required==}

Very concise name of the unit we use to measure the indicator values.

=== ":fontawesome-solid-list:  Guidelines"

	- Must not start with a capital letter.
	- Must not end with a period.
	- Must be empty if the indicator has no units.
	- Must be in plural.
	- Must be a metric unit when applicable.
	- Should not use symbols like “/”.
		- If it is a derived unit, use 'per' to denote a division, e.g. '... per hectare', or '... per person'.
	- Should be '%' for percentages.

=== ":material-note-edit: Examples"

	| :material-check: DO      | :material-close: DON'T  |
	| ----------- | --------- |
	| «`tonnes per hectare`» | «`tonnes/hectare`» |
	| «`kilowatts per person`» | «`kilowatts per capita`» |



---



For the complete metadata structure, see the [Dataset schema](https://files.ourworldindata.org/schemas/dataset-schema.json).


