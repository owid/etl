---
applyTo: "**/etl/steps/data/garden/**/*.meta.yml"
---

## Instructions

When filling metadata for garden datasets, follow these guidelines:

- Each metadata field must adhere to the schema rules defined below
- Use clear, precise language focused on data meaning rather than technical details
- For multiline text, use the `|-` YAML syntax and include linebreaks for readability
- Avoid using quotes (`'` or `"`) for strings unless the text contains quotes or special characters
- All descriptions should be concise and focused on what users need to know
- Always follow capitalization, formatting, and punctuation rules for each field
- Follow guidelines in the Schema below when filling the metadata


## Schema

"title": {
    "title": "Indicator's title",
    "type": "string",
    "description": "Title of the indicator, which is a few words definition of the indicator.",
    "examples": [
    "Number of neutron star mergers in the Milky Way",
    "Share of neutron star mergers that happen in the Milky Way",
    "Barley | 00000044 || Area harvested | 005312 || hectares"
    ],
    "examples_bad": [
    ["Number of neutron stars (NASA)"],
    [
        "Share of neutron star mergers that happen in the Milky Way (2023)"
    ],
    ["Barley"]
    ],
    "requirement_level": "required",
    "guidelines": [
    ["Must start with a capital letter."],
    ["Must not end with a period."],
    ["Must be one short sentence (a few words)."],
    [
        "For 'small datasets', this should be the publicly displayed title. For 'big datasets' (like FAOSTAT, with many dimensions), it can be less human-readable, optimized for internal searches (then, use `presentation.title_public` for the public title)."
    ],
    [
        "Should not mention other metadata fields like `producer` or `version`."
    ]
    ],
    "category": "metadata"
},
"description_short": {
    "title": "Indicator's short description",
    "type": "string",
    "description": "One or a few lines that complement the title to have a short description of the indicator.",
    "requirement_level": "required",
    "guidelines": [
    ["Must start with a capital letter."],
    ["Must end with a period."],
    [
        "Must be one short paragraph (for example suitable to fit in a chart subtitle)."
    ],
    [
        "Should not mention any other metadata fields (like information about the processing, or the origins, or the units).",
        {
        "type": "exceptions",
        "value": [
            "The unit can be mentioned if it is crucial for the description."
        ]
        }
    ]
    ],
    "category": "metadata"
},
"description_key": {
    "title": "Indicator's key information",
    "type": "array",
    "items": {
    "oneOf": [
        {
        "type": "string"
        },
        {
        "type": "array",
        "items": {
            "type": "string"
        }
        }
    ]
    },
    "description": "List of key pieces of information about the indicator.",
    "requirement_level": "recommended (for curated indicators)",
    "guidelines": [
    [
        "Must be a list of one or more short paragraphs.",
        {
        "type": "list",
        "value": [
            "Each paragraph must start with a capital letter.",
            "Each paragraph must end with a period."
        ]
        }
    ],
    [
        "Must not contain `description_short` (although there might be some overlap of information)."
    ],
    [
        "Should contain all the key information about the indicator (except that already given in `description_short`)."
    ],
    [
        "Should include the key information given in other fields like `grapher_config.subtitle` (if different from `description_short`) and `grapher_config.note`."
    ],
    [
        "Should not contain information about processing (which should be in `description_processing`)."
    ],
    [
        "Should only contain information that is key to the public.",
        {
        "type": "list",
        "value": [
            "Anything that is too detailed or technical should be left in the code."
        ]
        }
    ]
    ],
    "category": "metadata"
},
"description_processing": {
    "title": "Indicator's processing description",
    "type": "string",
    "description": "Relevant information about the processing of the indicator done by OWID.",
    "requirement_level": "required (if applicable)",
    "guidelines": [
    ["Must start with a capital letter."],
    ["Must end with a period."],
    [
        "Must be used if important editorial decisions have been taken during data processing."
    ],
    [
        "Must not be used to describe common processing steps like country harmonization."
    ],
    [
        "Should only contain key processing information to the public.",
        {
        "type": "list",
        "value": [
            "Anything that is too detailed or technical should be left in the code."
        ]
        }
    ]
    ],
    "category": "metadata"
},
"description_from_producer": {
    "title": "Indicator's description given by the producer",
    "type": "string",
    "description": "Description of the indicator written by the producer, if any was given.",
    "requirement_level": "recommended (if existing)",
    "guidelines": [
    ["Must start with a capital letter."],
    ["Must end with a period."],
    [
        "Should be identical to the producer's text, except for some formatting changes, typo corrections, or other appropriate minor edits."
    ],
    [
        "Should only be given if the producer clearly provides such definitions in a structured way. Avoid spending time searching for a definition given by the producer elsewhere."
    ]
    ],
    "category": "metadata"
},
"unit": {
    "title": "Indicator's unit",
    "type": "string",
    "description": "Very concise name of the unit we use to measure the indicator values.",
    "examples": ["tonnes per hectare", "kilowatts per person"],
    "examples_bad": [
    ["tonnes/hectare"],
    ["kilowatts per capita"]
    ],
    "requirement_level": "required",
    "guidelines": [
    ["Must not start with a capital letter."],
    ["Must not end with a period."],
    ["Must be empty if the indicator has no units."],
    ["Must be in plural."],
    ["Must be a metric unit when applicable."],
    [
        "Should not use symbols like “/”.",
        {
        "type": "list",
        "value": [
            "If it is a derived unit, use 'per' to denote a division, e.g. '... per hectare', or '... per person'."
        ]
        }
    ],
    ["Should be '%' for percentages."]
    ],
    "category": "metadata"
},
"short_unit": {
    "title": "Indicator's unit (short version)",
    "type": "string",
    "description": "Characters that represent the unit we use to measure the indicator value.",
    "examples": ["t/ha", "%", "kWh/person"],
    "examples_bad": [["t / ha"], ["pct"], ["pc"]],
    "requirement_level": "required",
    "guidelines": [
    [
        "Must follow the rules of capitalization of the International System of Units, when applicable."
    ],
    ["Must not end with a period."],
    ["Must be empty if the indicator has no units."],
    ["Should not contain spaces."],
    [
        "If, for clarity, we prefer to simplify the units in a chart, e.g. to show `kWh` instead of `kWh/person`, use `display.short_unit` for the simplified units, and keep the correct one in `indicator.short_unit` (and ensure there is no ambiguity in the chart)."
    ]
    ],
    "category": "metadata"
},
"processing_level": {
    "title": "Indicator's processing level",
    "oneOf": [
    {
        "enum": ["minor", "major"]
    },
    {
        "type": "string",
        "pattern": "<%"
    }
    ],
    "description": "Level of processing that the indicator values have experienced.",
    "requirement_level": "required (in the future this could be automatic).",
    "guidelines": [
    [
        "Must be `minor` if the indicator has undergone only minor operations since its origin:",
        {
        "type": "list",
        "value": [
            "Rename entities (e.g. countries or columns)",
            "Multiplication by a constant (e.g. unit change)",
            "Drop missing values."
        ]
        }
    ],
    [
        "Must be `major` if any other operation is used:",
        {
        "type": "list",
        "value": [
            "Data aggregates (e.g. sum data for continents or income groups)",
            "Operations between indicators (e.g. per capita, percentages, annual changes)",
            "Concatenation of indicators, etc."
        ]
        }
    ]
    ],
    "category": "metadata"
},
"license": {
    "title": "Indicator's final license",
    "type": "string",
    "description": "License of the indicator, which depends on the indicator's processing level and the origins' licenses.",
    "requirement_level": "required (in the future this could be automatic)",
    "guidelines": [
    [
        "If the indicator's `processing_level` is major, assign `CC BY 4.0`."
    ],
    [
        "If the indicator's `processing_level` is minor, choose the most strict license among the origins' `licenses`."
    ]
    ],
    "category": "metadata"
},
"sources": {
    "type": "array",
    "description": "List of all sources of the indicator. Automatically filled. NOTE: This is no longer in use, you should use origins.",
    "items": {
    "$ref": "definitions.json#/source"
    }
},
"origins": {
    "type": "array",
    "description": "List of all origins of the indicator.",
    "guidelines": [
    "**Note:** Origins should be propagated automatically from snapshots. Therefore, this field should only be manually filled out if automatic propagation fails."
    ],
    "items": {
    "$ref": "definitions.json#/origin"
    }
},
"display": {
    "$ref": "definitions.json#/display"
},
"presentation_license": {
    "type": "object",
    "description": "License to display for the indicator, overriding `license`.",
    "additionalProperties": false,
    "properties": {
    "url": {
        "description": "",
        "type": "string"
    },
    "name": {
        "description": "",
        "type": "string"
    }
    }
},
"sort": {
    "title": "Ordered categorical values for ordinal type",
    "description": "",
    "type": "array",
    "items": {
    "type": "string"
    }
},
"type": {
    "title": "Indicator type",
    "description": "Indicator type is usually automatically inferred from the data, but must be manually set for ordinal and categorical types.",
    "type": "string",
    "enum": [
    "float",
    "int",
    "mixed",
    "string",
    "ordinal",
    "categorical"
    ]
}
