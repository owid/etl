# Harmonization

## The meaning of data

In order to understand data within a single dataset, we want to know what is meant by the data.

For example, a `country` column containing the value `Korea` could be referring to South Korea, North Korea, or historical unified Korea as it existed a century ago, depending on the context and the intent of the data provider.

Harmonization is the editorial process by which we modify the indexing columns for a dataset to ensure that the data is consistent and unambiguous.

## What does Our World In Data harmonize?

Today, Our World In Data makes a best-effort to harmonize countries and regions. We strive to do this in a way that is consistent with the [ISO 3166-1 standard](https://en.wikipedia.org/wiki/ISO_3166-1), however we use custom editorial labels for countries and regions that are often shorter than those in the standard, in order to make data visualisations richer and more understandable.

Since we also present long-run datasets over multiple centuries, a time period in which national borders have changed, split and merged, we also make a best-effort attempt to harmonize the names of historical countries and regions that no longer exist and are not present in the ISO standard.

## How do we perform harmonization?

There are two methods that we use, both of which are semi-automated and involve some human judgement by our data managers.

### Command-line harmonization

The [etl](https://github.com/owid/etl) codebase contains an interactive `harmonize` command-line tool which can be used to harmonize a CSV file containing a column called `Country` or `Region`.

```
$ harmonize --help
Usage: harmonize [OPTIONS] DATA_FILE COLUMN OUTPUT_FILE [INSTITUTION]

  Given a data file in feather or CSV format, and the name of the column
  representing country or region names, interactively generate a JSON mapping
  from the given names to OWID's canonical names.

  When a name is ambiguous, you can use:

  n: to ignore and skip to the next one

  s: to suggest matches; you can also enter a manual match here

  q: to quit and save the partially complete mapping

  If a mapping file already exists, it will resume where the mapping file left
  off.

Options:
  --help  Show this message and exit.
```

### Using the Grapher admin

The [owid-grapher](https://github.com/owid/owid-grapher) codebase contains a interactive country harmonization tool that can be accessed at [http://localhost:3030/admin/standardize](http://localhost:3030/admin/standardize) when running the dev server.

To use the tool, you upload a CSV file containing a column called `Country`, and indicate the encoding of country names.

??? Tip "For staff"

    The interactive harmonization tool for staff is available at [https://owid.cloud/admin/standardize](https://owid.cloud/admin/standardize).

