---
applyTo: "**/snapshots/**/*.dvc"
---

## Overview

A Snapshot is a picture of a data product (e.g. a data CSV file) provided by an upstream data provider at a particular point in time. It is the entrypointto ETL and where we define metadata attributes of that picture. This is fundamental to ensure that the data is properly documented and that the metadata is propagated to the rest of the system.

## Instructions

When filling metadata for snapshots, follow these guidelines:

- Focus only on fields in the `meta.origin` section; other fields are handled elsewhere
- Use clear, precise language to describe the data source and its content
- For multiline text, use the `|-` YAML syntax (not just `|`), split long text into multiple lines, and include linebreaks for readability
- Avoid using quotes for strings unless they contain special characters
- **Do not** fill the `date_published` field
- Always follow capitalization, formatting, and punctuation rules for each field
- If `url_main` exists, go to that URL to get the context
- Follow guidelines in the schema below when filling the metadata


## Schema

"origin": {
    "type": "object",
    "description": "An indicator's origin is the information about the snapshot where the indicator's data and metadata came from. A snapshot is a subset of data (a 'slice') taken on a specific day from a data product (often a public dataset, but sometimes a paper or a database). The producer of the data product is typically an institution or a set of authors.\n\nA snapshot often coincides with the data product (e.g. the dataset is a public csv file, and we download the entire file). But sometimes the data product is a bigger object (e.g. a set of files, a paper or a database) and the snapshot is just a particular subset of the data product (e.g. one of the files, or a table from a paper, or the result of a query). The origin fields are the attributes of the `Origin` object in ETL.",
    "additionalProperties": false,
    "required": [
      "title",
      "date_published",
      "producer",
      "citation_full",
      "url_main",
      "date_accessed"
    ],
    "properties": {
      "title": {
        "title": "Title of the original data product",
        "type": "string",
        "description": "Title of the original data product.",
        "examples": [
          "Global Carbon Budget"
        ],
        "examples_bad": [
          [
            "Global Carbon Budget (fossil fuels)"
          ]
        ],
        "requirement_level": "required",
        "guidelines": [
          [
            "Must start with a capital letter."
          ],
          [
            "Must not end with a period."
          ],
          [
            "Must not mention other metadata fields like `producer` or `version_producer`.",
            {
              "type": "exceptions",
              "value": [
                "The name of the origin is well known and includes other metadata fields."
              ]
            }
          ],
          [
            "Should identify the data product, not the snapshot (i.e. the subset of data that we extract from the data product)."
          ],
          [
            "If the producer's data product has a well-known name, use that name exactly (except for minor changes like typos)."
          ],
          [
            "If the producer's data product does not have a well-known name, use a short sentence that describes its content."
          ]
        ],
        "category": "dataset"
      },
      "description": {
        "title": "Description of the data product",
        "type": "string",
        "description": "Description of the original data product.",
        "examples": [],
        "examples_bad": [],
        "requirement_level": "recommended",
        "guidelines": [
          [
            "Must start with a capital letter."
          ],
          [
            "Must end with a period."
          ],
          [
            "Must not mention other metadata fields like `producer` or `version_producer`.",
            {
              "type": "exceptions",
              "value": [
                "These other metadata fields are crucial in the description of the data product."
              ]
            }
          ],
          [
            "Should describe the data product, not the snapshot (i.e. the subset of data we extract from the data product)."
          ],
          [
            "Should ideally contain just one or a few paragraphs, that describe the data product succinctly."
          ],
          [
            "If the producer provides a good description, use that, either exactly or conveniently rephrased."
          ]
        ],
        "category": "dataset"
      },
      "date_published": {
        "title": "Date of publication of the original data",
        "type": "string",
        "description": "Exact day (or year, if exact day is unknown) when the producer's data (in its current version) was published.",
        "examples": [
          "2023-09-07",
          "2023"
        ],
        "examples_bad": [],
        "requirement_level": "required",
        "guidelines": [
          [
            "Must be a date with format `YYYY-MM-DD`, or, exceptionally, `YYYY`."
          ],
          [
            "Must be the date when the current version of the dataset was published (not when the dataset was first released)."
          ]
        ],
        "pattern": "(^\\d\\d\\d\\d\\-\\d\\d\\-\\d\\d$)|(^\\d{4}$)|(^{TODAY})",
        "errorMessage": "`date_published` must have format YYYY-MM-DD, YYYY or 'latest'",
        "category": "dataset"
      },
      "version_producer": {
        "type": [
          "string",
          "number"
        ],
        "title": "Version of the data product as given by the producer",
        "description": "Producer's version of the data product.",
        "requirement_level": "recommended (if existing)",
        "examples": [],
        "examples_bad": [],
        "guidelines": [
          [
            "Should be used if the producer specifies the version of the data product."
          ],
          [
            "Should follow the same naming as the producer, e.g. `v13`, `2023.a`, `version II`."
          ]
        ],
        "category": "dataset"
      },
      "producer": {
        "title": "Producer name",
        "type": "string",
        "description": "Name of the institution or the author(s) that produced the data product.",
        "examples": [
          "NASA",
          "World Bank",
          "Williams et al.",
          "van Haasteren et al.",
          "Williams and Jones"
        ],
        "examples_bad": [
          [
            "NASA (2023)",
            "N.A.S.A.",
            "N A S A",
            "National Aeronautics and Space Administration",
            "Our World in Data based on NASA"
          ],
          [
            "WB"
          ],
          [
            "Williams et al. (2023)",
            "Williams et al",
            "John Williams et al."
          ],
          [
            "Van Haasteren et al."
          ],
          [
            "Williams & Jones",
            "John Williams and Indiana Jones"
          ]
        ],
        "requirement_level": "required",
        "guidelines": [
          [
            "Must start with a capital letter.",
            {
              "type": "exceptions",
              "value": [
                "The name of the institution or the author must be spelled with small letter, e.g. `van Haasteren`."
              ]
            }
          ],
          [
            "Must not end with a period.",
            {
              "type": "exceptions",
              "value": [
                "When using `et al.` (for papers with multiple authors)."
              ]
            }
          ],
          [
            "Must not include a date or year."
          ],
          [
            "Must not mention `Our World in Data` or `OWID`."
          ],
          [
            "Must not include any semicolon `;`."
          ],
          [
            "Regarding authors:",
            {
              "type": "list",
              "value": [
                [
                  "One author: `Williams`."
                ],
                [
                  "Two authors: `Williams and Jones`."
                ],
                [
                  "Three or more authors: `Williams et al.`."
                ]
              ]
            }
          ],
          [
            "Regarding acronyms:",
            {
              "type": "list",
              "value": [
                "If the acronym is more well known than the full name, use just the acronym, e.g. `NASA`.",
                "If the acronym is not well known, use the full name, e.g. `Energy Institute`.",
                "If the institution explicitly asks, follow their guidelines, e.g. `Food and Agriculture Organization of the United Nations` (instead of `FAO`)."
              ]
            }
          ]
        ],
        "faqs": [
          {
            "question": "What should be the value if there are multiple producers?",
            "answer": "We don't have a clear guideline for this at the moment, and depending on the case you might want to specify all the producers. However, a good option is to use 'Various sources'.",
            "link": "https://github.com/owid/etl/discussions/1608"
          }
        ],
        "category": "citation"
      },
      "citation_full": {
        "title": "Full citation",
        "type": "string",
        "description": "Full citation of the data product. If the producer expressed how to cite them, we should follow their guidelines.",
        "examples": [],
        "examples_bad": [],
        "requirement_level": "required",
        "guidelines": [
          [
            "Must start with a capital letter."
          ],
          [
            "Must end with a period."
          ],
          [
            "Must include (wherever is appropriate) the year of publication, i.e. the year given in `date_published`."
          ],
          [
            "If the producer specified how to cite them, this field should be identical to the producer's text, except for some formatting changes, typo corrections, or other appropriate minor edits.",
            {
              "type": "list",
              "value": [
                "**Note:** This field can be as long as necessary to follow the producer's guidelines."
              ]
            }
          ],
          [
            "If the origin is the compilation of multiple sources, they can be added here as a list."
          ]
        ],
        "category": "citation"
      },
      "attribution": {
        "type": "string",
        "title": "Attribution",
        "description": "Citation of the data product to be used when the automatic format `producer (year)` needs to be overridden.",
        "requirement_level": "optional",
        "examples": [
          "Energy Institute - Statistical Review of World Energy (2023)"
        ],
        "examples_bad": [
          [
            "Statistical Review of World Energy, Energy Institute (2023)",
            "Statistical Review of World Energy (Energy Institute, 2023)"
          ]
        ],
        "guidelines": [
          [
            "Must start with a capital letter.",
            {
              "type": "exceptions",
              "value": [
                "The name of the institution or the author must be spelled with small letter, e.g. `van Haasteren`."
              ]
            }
          ],
          [
            "Must not end with a period."
          ],
          [
            "Must end with the year of `date_published` in parenthesis."
          ],
          [
            "Must not include any semicolon `;`."
          ],
          [
            "Should only be used if the automatic attribution format `producer (year)` is considered uninformative. For example, when the title of the data product is well known and should be cited along with the producer, or when the original version of the data product should also be mentioned."
          ],
          [
            "If this field is used to mention the data product, follow the preferred format `{producer} - {title} {version_producer} ({year})` (where `version_producer` may be omitted)."
          ],
          [
            "If the producer explicitly asked for a specific short citation, follow their guidelines and ignore the above."
          ]
        ],
        "category": "citation"
      },
      "attribution_short": {
        "type": "string",
        "title": "Attribution (shorter version)",
        "description": "Shorter version of `attribution` (without the year), usually an acronym of the producer, to be used in public places that are short on space.",
        "requirement_level": "recommended",
        "examples": [
          "FAO",
          "World Bank"
        ],
        "examples_bad": [
          [
            "UN FAO",
            "FAO (2023)"
          ],
          [
            "WB"
          ]
        ],
        "guidelines": [
          [
            "Must start with a capital letter.",
            {
              "type": "exceptions",
              "value": [
                "The name of the institution or the author must be spelled with small letter, e.g. `van Haasteren`."
              ]
            }
          ],
          [
            "Must not end with a period."
          ],
          [
            "Should refer to the producer or the data product (if well known), not the year or any other field."
          ],
          [
            "Should be an acronym, if the acronym is well-known, otherwise a brief name."
          ]
        ],
        "category": "citation"
      },
      "url_main": {
        "title": "URL of the main website of the data product",
        "type": "string",
        "description": "Producer's URL leading to the main website of the original data product.",
        "requirement_level": "required",
        "guidelines": [
          [
            "Must be a complete URL, i.e. `http...`."
          ],
          [
            "Should lead to a website where the data product is described."
          ]
        ],
        "pattern": "/(https:\/\/www\\.|http:\/\/www\\.|https:\/\/|http:\/\/)?[a-zA-Z]{2,}(\\.[a-zA-Z]{2,})(\\.[a-zA-Z]{2,})?\/[a-zA-Z0-9]{2,}|((https:\/\/www\\.|http:\/\/www\\.|https:\/\/|http:\/\/)?[a-zA-Z]{2,}(\\.[a-zA-Z]{2,})(\\.[a-zA-Z]{2,})?)|(https:\/\/www\\.|http:\/\/www\\.|https:\/\/|http:\/\/)?[a-zA-Z0-9]{2,}\\.[a-zA-Z0-9]{2,}\\.[a-zA-Z0-9]{2,}(\\.[a-zA-Z0-9]{2,})?/g;",
        "errorMessage": "'url_main' must be valid URL",
        "examples": [
          "https://data.some_institution.com/dataset_12"
        ],
        "examples_bad": [],
        "category": "files"
      },
      "url_download": {
        "title": "URL to download the producer's data",
        "type": "string",
        "description": "Producer's URL that directly downloads their data as a single file.",
        "pattern": "/(https:\/\/www\\.|http:\/\/www\\.|https:\/\/|http:\/\/)?[a-zA-Z]{2,}(\\.[a-zA-Z]{2,})(\\.[a-zA-Z]{2,})?\/[a-zA-Z0-9]{2,}|((https:\/\/www\\.|http:\/\/www\\.|https:\/\/|http:\/\/)?[a-zA-Z]{2,}(\\.[a-zA-Z]{2,})(\\.[a-zA-Z]{2,})?)|(https:\/\/www\\.|http:\/\/www\\.|https:\/\/|http:\/\/)?[a-zA-Z0-9]{2,}\\.[a-zA-Z0-9]{2,}\\.[a-zA-Z0-9]{2,}(\\.[a-zA-Z0-9]{2,})?/g;",
        "errorMessage": "'url_download' must be a valid URL",
        "requirement_level": "required (if existing)",
        "guidelines": [
          [
            "Must be a complete URL or S3 URI, i.e. `http...`."
          ],
          [
            "Must be a direct download link.",
            {
              "type": "list",
              "value": [
                "The URL must not lead to a website that requires user input to download the dataset. If there is no direct download URL, this field should be empty."
              ]
            }
          ]
        ],
        "examples": [
          "https://data.some_institution.com/dataset_12/data.csv",
          "s3://owid-private/data.csv"
        ],
        "examples_bad": [],
        "category": "files"
      },
      "date_accessed": {
        "title": "Date when we accessed the producer's original data",
        "type": "string",
        "format": "date",
        "description": "Exact day when the producer's data (in its current version) was downloaded by OWID.",
        "examples": [
          "2023-09-07"
        ],
        "examples_bad": [],
        "requirement_level": "required",
        "guidelines": [
          [
            "Must be a date with format `YYYY-MM-DD`."
          ],
          [
            "Must be the date when the current version of the producer's data was accessed (not any other previous version)."
          ]
        ],
        "pattern": "(^\\d\\d\\d\\d\\-\\d\\d\\-\\d\\d$)|(^{TODAY})",
        "errorMessage": "`date_accessed` must have format YYYY-MM-DD",
        "category": "files"
      },
      "title_snapshot": {
        "title": "Title of the snapshot",
        "type": "string",
        "description": "Title of the snapshot (i.e. the subset of data that we extract from the data product).",
        "examples": [
          "Global Carbon Budget - Fossil fuels",
          "Neutron star mergers"
        ],
        "examples_bad": [
          [
            "Global Carbon Budget"
          ],
          [
            "Neutron star mergers (NASA, 2023)",
            "Data on neutron star mergers",
            "Neutron star mergers dataset"
          ]
        ],
        "requirement_level": "required (if different from `title`)",
        "guidelines": [
          [
            "Must start with a capital letter."
          ],
          [
            "Must not end with a period."
          ],
          [
            "Must not mention other metadata fields like `producer` or `version_producer`.",
            {
              "type": "exceptions",
              "value": [
                "The name of the origin is well known and includes other metadata fields.",
                [
                  "The producer's data product has a well-known name, and the snapshot is a specific slice of the data product. If so, use the format 'Data product - Specific slice'.",
                  {
                    "type": "list",
                    "value": [
                      "**Note:** This means that the title of the snapshot may contain the title of the data product."
                    ]
                  }
                ]
              ]
            }
          ],
          [
            "Must not include any semicolon `;`."
          ],
          [
            "Should only be used when the snapshot does not coincide with the entire data product."
          ],
          [
            "Should not include words like `data`, `dataset` or `database`, unless that's part of a well-known name of the origin."
          ],
          [
            "If the producer's data product does not have a well-known name, use a short sentence that describes the snapshot."
          ]
        ],
        "category": "dataset"
      },
      "description_snapshot": {
        "title": "Description of the snapshot",
        "type": "string",
        "description": "Additional information to append to the description of the data product, in order to describe the snapshot (i.e. the subset of data that we extract from the data product).",
        "requirement_level": "recommended (if the data product and snapshot do not coincide)",
        "guidelines": [
          [
            "Must start with a capital letter."
          ],
          [
            "Must end with a period."
          ],
          [
            "Should be defined only if the data product and the snapshot do not coincide."
          ],
          [
            "Should not repeat information given in `description` (the description of the data product)."
          ],
          [
            "Should not mention other metadata fields.",
            {
              "type": "list",
              "value": [
                "If fields like `producer` or `date_published` are mentioned, placeholders should be used."
              ]
            }
          ]
        ],
        "examples": [],
        "examples_bad": [],
        "category": "dataset"
      },
      "license": {
        "$ref": "#/license"
      }
    }
  },
