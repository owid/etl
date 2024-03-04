---
tags:
  - Metadata
---

# Metadata reference

<div class="grid cards" markdown>

- __[Indicator](#variable)__ (variable)
- __[Origin](#origin)__
- __[Table](#tables)__
- __[Dataset](#dataset)__
</div>


## `variable`

An indicator (also commonly called 'variable') is a collection of data points (usually a time series) with metadata. The indicator metadata fields are the attributes of the `VariableMeta` object in ETL.

### `variable.description_from_producer`

*type*: `string` | recommended (if existing)

Description of the indicator written by the producer, if any was given.

=== ":fontawesome-solid-list:  Guidelines"
        
	- Must start with a capital letter.
	- Must end with a period.
	- Should be identical to the producer's text, except for some formatting changes, typo corrections, or other appropriate minor edits.
	- Should only be given if the producer clearly provides such definitions in a structured way. Avoid spending time searching for a definition given by the producer elsewhere.
    



---


### `variable.description_key`

*type*: `array`, `array` | recommended (for curated indicators)

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


### `variable.description_processing`

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


### `variable.description_short`

*type*: `string` | {==required==}

One or a few lines that complement the title to have a short description of the indicator.

=== ":fontawesome-solid-list:  Guidelines"
        
	- Must start with a capital letter.
	- Must end with a period.
	- Must be one short paragraph (for example suitable to fit in a chart subtitle).
	- Should not mention any other metadata fields (like information about the processing, or the origins, or the units). **Exceptions:**
		- The unit can be mentioned if it is crucial for the description.
    



---


### `variable.display`

We keep display for the time being as the 'less powerful sibling' of grapher config.

#### `variable.display.color`

*type*: `string`

Color to use for the indicator in e.g. line charts.





---


#### `variable.display.conversionFactor`

*type*: `number`

Conversion factor to apply to indicator values.

=== ":fontawesome-solid-list:  Guidelines"
        
	- **Note:** We should avoid using this, and instead convert data and units (and possibly other metadata fields where the units are mentioned) consistently in the ETL grapher step.
    



---


#### `variable.display.description`

*type*: `string`

Description to display for the indicator, to replace the indicator's `description`.





---


#### `variable.display.entityAnnotationsMap`

*type*: `string`

Entity annotations





---


#### `variable.display.includeInTable`

*type*: `boolean`

Whether to render this indicator in the table sheet.





---


#### `variable.display.isProjection`

*type*: `boolean`

Indicates if this time series is a forward projection (if so then this is rendered differently in e.g. line charts).





---


#### `variable.display.name`

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


#### `variable.display.numDecimalPlaces`

*type*: `integer`

Number of decimal places to show in charts (and in the table tab).





---


#### `variable.display.shortUnit`

*type*: `string`

Short unit to use in charts instead of the indicator's `short_unit`.





---


#### `variable.display.tableDisplay`

Configuration for the table tab for this indicator, with options `hideAbsoluteChange` and `hideRelativeChange`.

##### `variable.display.tableDisplay.hideAbsoluteChange`

*type*: `boolean`

Whether to hide the absolute change.





---


##### `variable.display.tableDisplay.hideRelativeChange`

*type*: `boolean`

Whether to hide the relative change.





---


#### `variable.display.tolerance`

*type*: `integer`

Tolerance (in years or days) to use in charts. If data points are missing, the closest data point will be shown, if it lies within the specified tolerance.





---


#### `variable.display.unit`

*type*: `string`

Unit to use in charts instead of the indicator's `unit`.





---


#### `variable.display.yearIsDay`

*type*: `boolean`

Switch to indicate if the number in the year column represents a day (since zeroDay) or a year.





---


#### `variable.display.zeroDay`

*type*: `string`

ISO date day string for the starting date if `yearIsDay` is `True`.





---


### `variable.license`

*type*: `string` | {==required (in the future this could be automatic)==}

License of the indicator, which depends on the indicator's processing level and the origins' licenses.

=== ":fontawesome-solid-list:  Guidelines"
        
	- If the indicator's `processing_level` is major, assign `CC BY 4.0`.
	- If the indicator's `processing_level` is minor, choose the most strict license among the origins' `licenses`.
    



---


### `variable.origins`

*type*: `array`

List of all origins of the indicator.

=== ":fontawesome-solid-list:  Guidelines"
        
	- **Note:** Origins should be propagated automatically from snapshots. Therefore, this field should only be manually filled out if automatic propagation fails.
    



---


### `variable.presentation`

An indicator's presentation defines how the indicator's metadata will be shown on our website (e.g. in data pages). The indicator presentation metadata fields are the attributes of the `VariablePresentationMeta`object in ETL.

#### `variable.presentation.attribution`

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


#### `variable.presentation.attribution_short`

*type*: `string` | recommended (for curated indicators)

Very short citation of the indicator's main producer(s).

=== ":fontawesome-solid-list:  Guidelines"
        
	- Must start with a capital letter. **Exceptions:**
		- The name of the institution or the author must be spelled with small letter, e.g. `van Haasteren`.
	- Must not end in a period.
	- Should be very short.
	- Should be used if the automatic concatenation of origin's `attribution_short` are too long. In those cases, choose the most important `attribution` (e.g. the main producer of the data).
    



---


#### `variable.presentation.faqs`

*type*: `array` | recommended (for curated indicators)

List of references to questions in an FAQ google document, relevant to the indicator.

=== ":fontawesome-solid-list:  Guidelines"
        
	- Each reference must contain `fragment_id` (question identifier) and `gdoc_id` (document identifier).
    



---


#### `variable.presentation.grapher_config`

Our World in Data grapher configuration. Most of the fields can be left empty and will be filled with reasonable default values.

Find more details on its attributes [here](https://files.ourworldindata.org/schemas/grapher-schema.003.json).

#### `variable.presentation.title_public`

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


#### `variable.presentation.title_variant`

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


#### `variable.presentation.topic_tags`

*type*: `array` | recommended (for curated indicators)

List of topics where the indicator is relevant.

=== ":fontawesome-solid-list:  Guidelines"
        
	- Must be an existing topic tag, and be spelled correctly (see the list of topic tags in [datasette](http://datasette-private/owid?sql=SELECT+tags.%60name%60+from+tags+where+slug+is+not+null+ORDER+BY+tags.%60name%60%0D%0A) (requires Tailscale).
	- The first tag must correspond to the most relevant topic page (since that topic page will be used in citations of this indicator).
	- Should contain 1, 2, or at most 3 tags.
    



---


### `variable.presentation_license`

License to display for the indicator, overriding `license`.

#### `variable.presentation_license.name`

*type*: `string`







---


#### `variable.presentation_license.url`

*type*: `string`







---


### `variable.processing_level`

*type*: `string` | {==required (in the future this could be automatic).==}

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


### `variable.short_unit`

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


### `variable.sources`

*type*: `array`

List of all sources of the indicator. Automatically filled. NOTE: This is no longer in use, you should use origins.





---


### `variable.title`

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


### `variable.unit`

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


## `origin`

An indicator's origin is the information about the snapshot where the indicator's data and metadata came from. A snapshot is a subset of data (a 'slice') taken on a specific day from a data product (often a public dataset, but sometimes a paper or a database). The producer of the data product is typically an institution or a set of authors.

A snapshot often coincides with the data product (e.g. the dataset is a public csv file, and we download the entire file). But sometimes the data product is a bigger object (e.g. a set of files, a paper or a database) and the snapshot is just a particular subset of the data product (e.g. one of the files, or a table from a paper, or the result of a query). The origin fields are the attributes of the `Origin` object in ETL.

### `origin.attribution`

*type*: `string` | optional

Citation of the data product to be used when the automatic format `producer (year)` needs to be overridden.

=== ":fontawesome-solid-list:  Guidelines"
        
	- Must start with a capital letter. **Exceptions:**
		- The name of the institution or the author must be spelled with small letter, e.g. `van Haasteren`.
	- Must not end with a period.
	- Must end with the year of `date_published` in parenthesis.
	- Must not include any semicolon `;`.
	- Should only be used if the automatic attribution format `producer (year)` is considered uninformative. For example, when the title of the data product is well known and should be cited along with the producer, or when the original version of the data product should also be mentioned.
	- If this field is used to mention the data product, follow the preferred format `{producer} - {title} {version_producer} ({year})` (where `version_producer` may be omitted).
	- If the producer explicitly asked for a specific short citation, follow their guidelines and ignore the above.
    
=== ":material-note-edit: Examples"
        
	| :material-check: DO      | :material-close: DON'T  |
	| ----------- | --------- |
	| «`Energy Institute - Statistical Review of World Energy (2023)`» | «`Statistical Review of World Energy, Energy Institute (2023)`», «`Statistical Review of World Energy (Energy Institute, 2023)`» |
    


---


### `origin.attribution_short`

*type*: `string` | recommended

Shorter version of `attribution` (without the year), usually an acronym of the producer, to be used in public places that are short on space.

=== ":fontawesome-solid-list:  Guidelines"
        
	- Must start with a capital letter. **Exceptions:**
		- The name of the institution or the author must be spelled with small letter, e.g. `van Haasteren`.
	- Must not end with a period.
	- Should refer to the producer or the data product (if well known), not the year or any other field.
	- Should be an acronym, if the acronym is well-known, otherwise a brief name.
    
=== ":material-note-edit: Examples"
        
	| :material-check: DO      | :material-close: DON'T  |
	| ----------- | --------- |
	| «`FAO`» | «`UN FAO`», «`FAO (2023)`» |
	| «`World Bank`» | «`WB`» |
    


---


### `origin.citation_full`

*type*: `string` | {==required==}

Full citation of the data product. If the producer expressed how to cite them, we should follow their guidelines.

=== ":fontawesome-solid-list:  Guidelines"
        
	- Must start with a capital letter.
	- Must end with a period.
	- Must include (wherever is appropriate) the year of publication, i.e. the year given in `date_published`.
	- If the producer specified how to cite them, this field should be identical to the producer's text, except for some formatting changes, typo corrections, or other appropriate minor edits.
		- **Note:** This field can be as long as necessary to follow the producer's guidelines.
	- If the origin is the compilation of multiple sources, they can be added here as a list.
    



---


### `origin.date_accessed`

*type*: `string` | {==required==}

Exact day when the producer's data (in its current version) was downloaded by OWID.

=== ":fontawesome-solid-list:  Guidelines"
        
	- Must be a date with format `YYYY-MM-DD`.
	- Must be the date when the current version of the producer's data was accessed (not any other previous version).
    
=== ":material-note-edit: Examples"
        

	:material-check: «`2023-09-07`» 
    


---


### `origin.date_published`

*type*: `string` | {==required==}

Exact day (or year, if exact day is unknown) when the producer's data (in its current version) was published.

=== ":fontawesome-solid-list:  Guidelines"
        
	- Must be a date with format `YYYY-MM-DD`, or, exceptionally, `YYYY`.
	- Must be the date when the current version of the dataset was published (not when the dataset was first released).
    
=== ":material-note-edit: Examples"
        

	:material-check: «`2023-09-07`» 

	:material-check: «`2023`» 
    


---


### `origin.description`

*type*: `string` | recommended

Description of the original data product.

=== ":fontawesome-solid-list:  Guidelines"
        
	- Must start with a capital letter.
	- Must end with a period.
	- Must not mention other metadata fields like `producer` or `version_producer`. **Exceptions:**
		- These other metadata fields are crucial in the description of the data product.
	- Should describe the data product, not the snapshot (i.e. the subset of data we extract from the data product).
	- Should ideally contain just one or a few paragraphs, that describe the data product succinctly.
	- If the producer provides a good description, use that, either exactly or conveniently rephrased.
    



---


### `origin.description_snapshot`

*type*: `string` | recommended (if the data product and snapshot do not coincide)

Additional information to append to the description of the data product, in order to describe the snapshot (i.e. the subset of data that we extract from the data product).

=== ":fontawesome-solid-list:  Guidelines"
        
	- Must start with a capital letter.
	- Must end with a period.
	- Should be defined only if the data product and the snapshot do not coincide.
	- Should not repeat information given in `description` (the description of the data product).
	- Should not mention other metadata fields.
		- If fields like `producer` or `date_published` are mentioned, placeholders should be used.
    



---


### `origin.license`

An origin's license is the license, assigned by a producer, of the data product from where we extracted the indicator's original data and metadata.

#### `origin.license.name`

*type*: `string` | {==required==}

Name of the license. Find more details on licensing at https://creativecommons.org/share-your-work/cclicenses/.

=== ":fontawesome-solid-list:  Guidelines"
        
	- If it's a standard license, e.g. CC, it should be one of the acronyms in the examples below.
	- If the license is CC, but the version is not specified, assume 4.0.
	- If it's a custom license defined by the producer, it should follow the producer's text.
	- When the license of an external dataset is not specified, temporarily assume `CC BY 4.0`. Contact the producer before publishing.
		- If there is no response after a few days, ask Ed or Este and decide on a case-by-case basis.
    
=== ":material-note-edit: Examples"
        

	:material-check: «`Public domain`» 

	:material-check: «`CC0`» 

	:material-check: «`PDM`» 

	:material-check: «`CC BY 4.0`» 

	:material-check: «`CC BY-SA 4.0`» 

	:material-check: «`© GISAID 2023`» 
    


---


#### `origin.license.url`

*type*: `string` | {==required (if existing)==}

URL leading to the producer's website where the dataset license is specified.

=== ":fontawesome-solid-list:  Guidelines"
        
	- Must be a complete URL, i.e. `http...` .
	- Must not lead to a Creative Commons website or any other generic page, but to the place where the producer specifies the license of the data.
	- If the license is specified inside, say, a PDF document, the URL should be the download link of that document.
	- When the license of an external dataset is not specified, leave `url` empty.
		- Do not use the URL of the main page of the dataset if the license is not mentioned anywhere.
    



---


### `origin.producer`

*type*: `string` | {==required==}

Name of the institution or the author(s) that produced the data product.

=== ":fontawesome-solid-list:  Guidelines"
        
	- Must start with a capital letter. **Exceptions:**
		- The name of the institution or the author must be spelled with small letter, e.g. `van Haasteren`.
	- Must not end with a period. **Exceptions:**
		- When using `et al.` (for papers with multiple authors).
	- Must not include a date or year.
	- Must not mention `Our World in Data` or `OWID`.
	- Must not include any semicolon `;`.
	- Regarding authors:
		- One author: `Williams`.
		- Two authors: `Williams and Jones`.
		- Three or more authors: `Williams et al.`.
	- Regarding acronyms:
		- If the acronym is more well known than the full name, use just the acronym, e.g. `NASA`.
		- If the acronym is not well known, use the full name, e.g. `Energy Institute`.
		- If the institution explicitly asks, follow their guidelines, e.g. `Food and Agriculture Organization of the United Nations` (instead of `FAO`).
    
=== ":material-note-edit: Examples"
        
	| :material-check: DO      | :material-close: DON'T  |
	| ----------- | --------- |
	| «`NASA`» | «`NASA (2023)`», «`N.A.S.A.`», «`N A S A`», «`National Aeronautics and Space Administration`», «`Our World in Data based on NASA`» |
	| «`World Bank`» | «`WB`» |
	| «`Williams et al.`» | «`Williams et al. (2023)`», «`Williams et al`», «`John Williams et al.`» |
	| «`van Haasteren et al.`» | «`Van Haasteren et al.`» |
	| «`Williams and Jones`» | «`Williams & Jones`», «`John Williams and Indiana Jones`» |
    
=== ":material-chat-question: FAQs"
        
	**_What should be the value if there are multiple producers?_**

	We don't have a clear guideline for this at the moment, and depending on the case you might want to specify all the producers. However, a good option is to use 'Various sources'.

	[See discussion on Github](https://github.com/owid/etl/discussions/1608)
    

---


### `origin.title`

*type*: `string` | {==required==}

Title of the original data product.

=== ":fontawesome-solid-list:  Guidelines"
        
	- Must start with a capital letter.
	- Must not end with a period.
	- Must not mention other metadata fields like `producer` or `version_producer`. **Exceptions:**
		- The name of the origin is well known and includes other metadata fields.
	- Should identify the data product, not the snapshot (i.e. the subset of data that we extract from the data product).
	- If the producer's data product has a well-known name, use that name exactly (except for minor changes like typos).
	- If the producer's data product does not have a well-known name, use a short sentence that describes its content.
    
=== ":material-note-edit: Examples"
        
	| :material-check: DO      | :material-close: DON'T  |
	| ----------- | --------- |
	| «`Global Carbon Budget`» | «`Global Carbon Budget (fossil fuels)`» |
    


---


### `origin.title_snapshot`

*type*: `string` | {==required (if different from `title`)==}

Title of the snapshot (i.e. the subset of data that we extract from the data product).

=== ":fontawesome-solid-list:  Guidelines"
        
	- Must start with a capital letter.
	- Must not end with a period.
	- Must not mention other metadata fields like `producer` or `version_producer`. **Exceptions:**
		- The name of the origin is well known and includes other metadata fields.
		- The producer's data product has a well-known name, and the snapshot is a specific slice of the data product. If so, use the format 'Data product - Specific slice'.
			- **Note:** This means that the title of the snapshot may contain the title of the data product.
	- Must not include any semicolon `;`.
	- Should only be used when the snapshot does not coincide with the entire data product.
	- Should not include words like `data`, `dataset` or `database`, unless that's part of a well-known name of the origin.
	- If the producer's data product does not have a well-known name, use a short sentence that describes the snapshot.
    
=== ":material-note-edit: Examples"
        
	| :material-check: DO      | :material-close: DON'T  |
	| ----------- | --------- |
	| «`Global Carbon Budget - Fossil fuels`» | «`Global Carbon Budget`» |
	| «`Neutron star mergers`» | «`Neutron star mergers (NASA, 2023)`», «`Data on neutron star mergers`», «`Neutron star mergers dataset`» |
    


---


### `origin.url_download`

*type*: `string` | {==required (if existing)==}

Producer's URL that directly downloads their data as a single file.

=== ":fontawesome-solid-list:  Guidelines"
        
	- Must be a complete URL or S3 URI, i.e. `http...`.
	- Must be a direct download link.
		- The URL must not lead to a website that requires user input to download the dataset. If there is no direct download URL, this field should be empty.
    
=== ":material-note-edit: Examples"
        

	:material-check: «`https://data.some_institution.com/dataset_12/data.csv`» 

	:material-check: «`s3://owid-private/data.csv`» 
    


---


### `origin.url_main`

*type*: `string` | {==required==}

Producer's URL leading to the main website of the original data product.

=== ":fontawesome-solid-list:  Guidelines"
        
	- Must be a complete URL, i.e. `http...`.
	- Should lead to a website where the data product is described.
    
=== ":material-note-edit: Examples"
        

	:material-check: «`https://data.some_institution.com/dataset_12`» 
    


---


### `origin.version_producer`

*type*: `string`, `number` | recommended (if existing)

Producer's version of the data product.

=== ":fontawesome-solid-list:  Guidelines"
        
	- Should be used if the producer specifies the version of the data product.
	- Should follow the same naming as the producer, e.g. `v13`, `2023.a`, `version II`.
    



---


## `table`

A table is a collection of indicators that share the same index. The table metadata fields are the attributes of the `TableMeta` object in ETL.

### `table.common`



### `table.description`

*type*: `string` | recommended (often automatic)

Description of the table (mostly for internal purposes, or for users of our data catalog) which is a one- (or a few) paragraph description of the table.

=== ":fontawesome-solid-list:  Guidelines"
        
	- Must start with a capital letter.
	- Must end with a period.
	- Should not mention other metadata fields (e.g. `producer` or `date_published`). **Exceptions:**
		- The other metadata fields are crucial in the description of the data product.
	- Should ideally contain just one or a few paragraphs, that describe its content succinctly.
	- Should be used only to override the automatic description (which usually is the description of the origin). For example, use it when the table has multiple origins.
    



---


### `table.title`

*type*: `string` | {==required (often automatic)==}

Title of the table (mostly for internal purposes, or for users of our data catalog) which is a few words description of the table.

=== ":fontawesome-solid-list:  Guidelines"
        
	- Must start with a capital letter.
	- Must not end with a period.
	- Should identify the table.
	- Should be used only to override the automatic title (which usually is the title of the origin). For example, use it when the table has multiple origins.
    



---


### `table.variables`

An indicator (also commonly called 'variable') is a collection of data points (usually a time series) with metadata. The indicator metadata fields are the attributes of the `VariableMeta` object in ETL.

## `dataset`

An ETL dataset is a collection of tables. The dataset metadata fields are the attributes of the `DatasetMeta` object in ETL.

### `dataset.description`

*type*: `string` | recommended (often automatic)

Description of the dataset (mostly for internal purposes, or for users of our data catalog) which is a one- (or a few) paragraph description of the content of the tables.

=== ":fontawesome-solid-list:  Guidelines"
        
	- Must start with a capital letter.
	- Must end with a period.
	- Should not mention other metadata fields (e.g. `producer` or `date_published`). **Exceptions:**
		- The other metadata fields are crucial in the description of the data product.
	- Should describe the dataset (i.e. the collection of tables resulting from one or more original data products).
	- Should ideally contain just one or a few paragraphs, that describe its content succinctly.
	- Should be used only to override the automatic description (which usually is the description of the containing table). For example, use it when the dataset contains multiple tables.
    



---


### `dataset.licenses`

*type*: `array`

List of all licenses that have been involved in the processing history of the indicators in this dataset.

=== ":fontawesome-solid-list:  Guidelines"
        
	- **Note:** Licenses should be propagated automatically from snapshots. Therefore, this field should only be manually filled out if automatic propagation fails. In the near future, this field may not even exist, since `licenses` should only exist inside `origins`.
    



---


### `dataset.non_redistributable`

*type*: `boolean`

Whether the dataset is non-redistributable, and therefore data should not be downloadable from the chart.





---


### `dataset.sources`

*type*: `array`

(DEPRECATED, no longer in use). List of all sources of the indicators in this dataset.





---


### `dataset.title`

*type*: `string` | {==required (often automatic)==}

Title of the dataset (mostly for internal purposes, or for users of our data catalog) which is a one-line description of the dataset.

=== ":fontawesome-solid-list:  Guidelines"
        
	- Must start with a capital letter.
	- Must not end with a period.
	- Should identify the dataset (i.e. the collection of tables resulting from one or more original data products).
	- Should be used only to override the automatic title (which usually is the title of the containing table). For example, use it when the dataset contains multiple tables.
    



---


### `dataset.update_period_days`

*type*: `integer` | {==required==}

Expected number of days between consecutive updates of this dataset by OWID, typically `30`, `90` or `365`. Set to `0` if we do not have the intention to update it.

=== ":fontawesome-solid-list:  Guidelines"
        
	- Must be defined in the garden step.
	- Must be an integer.
	- Must specify the update period of OWID's data, not the producer's data (although they may often coincide, e.g. `365`).
    
=== ":material-note-edit: Examples"
        
	| :material-check: DO      | :material-close: DON'T  |
	| ----------- | --------- |
	| «`7`» | «`2023-01-07`» |
	| «`30`» | «`monthly`» |
	| «`90`» | «`0.2`» |
	| «`365`» | «`1/365`» |
    


---


