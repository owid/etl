A crucial step in the ETL process is harmonizing country names. This is because different datasets or providers may use different names to refer to the same country. We want to keep country names consistent, and use names from our standardised list.

!!! info

    All our standardised country names are defined in our regions dataset (see [this YAML file](https://github.com/owid/etl/blob/master/etl/steps/data/garden/regions/2023-01-01/regions.yml)).

Typically, harmonizing country names is done after the Meadow step and before (or during) the Garden step, and is is consolidated into a JSON dictionary, which maps the source's country names to our standard names:

```json
// some_step.countries.json
{
  "United States of America": "United States",
  "Congo, Democratic Republic of": "Democratic Republic of Congo",
  "DPRK": "North Korea"
}
```

Our harmonizer is available from [Wizard](#using-wizard), the [terminal with our CLI](#using-our-cli), and the [interactive shell](#using-the-interactive-shell).

## Methodology

Harmonization is the editorial process by which we modify the indexing columns for a dataset to ensure that the data is consistent and unambiguous.

Harmonizing a country name can sometimes be done automatically, based on mappings done in the past. However, in many cases, manual intervention is needed. For example, a `country` column containing the value `Korea` could be referring to South Korea, North Korea, or historical unified Korea, depending on the context and the intent of the data provider. In such case, human judgement is needed.

We strive to harmonize country names in a way that is consistent with the [ISO 3166-1 standard](https://en.wikipedia.org/wiki/ISO_3166-1), however we use custom editorial labels for countries and regions that are often shorter than those in the standard, in order to make data visualisations richer and more understandable.

Since we also present long-run datasets over multiple centuries, a time period in which national borders have changed, split and merged, we also make a best-effort attempt to harmonize the names of historical countries and regions that no longer exist and are not present in the ISO standard.

Our harmonization tool relies on [rapidfuzz](https://github.com/rapidfuzz/RapidFuzz).

## Using Wizard

After generating a Meadow dataset, one can use the Harmonizer page in Wizard to generate a JSON mapping file for the Garden step.

<figure markdown="span">
  ![Chart Upgrader](../assets/harmonizer-wizard.png)
  <figcaption>Harmonizer page in Wizard. You can choose default names from the dropdowns or enter custm names.</figcaption>
</figure>

## Using our CLI

Our [ETL CLI](../etl-cli.md) contains an interactive `harmonize` command-line tool which can be used to harmonize a CSV file that contains a column with country names.

```
~ etl harmonize --help

Usage: etl harmonize [OPTIONS] DATA_FILE COLUMN OUTPUT_FILE

Generate a dictionary with the mapping of country names to OWID's canonical names.
Harmonize the country names in COLUMN of a DATA_FILE (CSV or feather) and save the mapping
to OUTPUT_FILE as a JSON file. The harmonization process is done according to OWID's
canonical country names.

The harmonization process is done interactively, where the user is prompted with a list of
ambiguous country names and asked to select the correct country name from a list of
suggestions (ranked by similarity).

When the mapping is ambiguous, you can use:

• Choose Option [custom] to enter a custom name.
• Type Ctrl-C to exit and save the partially complete mapping

If a mapping file already exists, it will resume where the mapping file left off.

╭─ Options ────────────────────────────────────────────────────────────────────────────────╮
│ --institution      -i  TEXT     Append '(institution)' to countries                      │
│ --num-suggestions  -n  INTEGER  Number of suggestions to show per entity. Default is 5   │
│                                 [default: 5]                                             │
│ --help                          Show this message and exit.                              │
╰──────────────────────────────────────────────────────────────────────────────────────────╯
```

As an example, start the harmonization interactive session for table `undp_hdr` from dataset `meadow/un/2024-04-09/undp_hdr`, which has `country` column with the raw country names:

```bash
poetry run etl harmonize data/meadow/un/2024-04-09/undp_hdr/undp_hdr.feather country mapping.json
206 countries/regions to harmonize
  └ 188 automatically matched
  └ 18 ambiguous countries/regions

Beginning interactive harmonization...
  Select [skip] to skip a country/region mapping
  Select [custom] to enter a custom name

? [1/18] Arab States: (Use shortcuts or arrow keys)
 » 1) Yemen Arab Republic
   2) United States Virgin Islands
   3) United States Minor Outlying Islands
   4) United States
   5) United Arab Emirates
   6) [custom]
   7) [skip]
```

The output mapping is saved in `mapping.json`. If this file existed before, it will resume teh session from where it left off.

## Using the interactive shell

If you are editing a step script in VS Code, it can be helpful to have access to the harmonizer tool from the interactive shell.

You can simply invoke it with the following code:

```python
from etl.harmonize import harmonize_ipython
harmonize_ipython(
   tb,
   paths=paths,
)
```

This assumes that you have a `PathFinder` instance defined as `paths` and that `tb` has a column `"country"` to harmonize. If you want to use a different column, you can pass it as the `column` argument.

```python
from etl.harmonize import harmonize_ipython
harmonize_ipython(
   tb,
   column="region",
   paths=paths,
)
```

If you want to export the mapping to a different file, you can pass the `output_file` argument.

```python
from etl.harmonize import harmonize_ipython
harmonize_ipython(
   tb,
   output_file="output/file/path.json",
)
```

The harmonizer will present a form with a list of ambiguous country names and ask you to select the correct country name from a list of suggestions (ranked by similarity). This is similar to the experience that you'd get from Wizard or the CLI.

<figure markdown="span">
  ![Chart Upgrader](../assets/harmonize-ipython.gif)
  <figcaption>Harmonizer interactive tool in VS Code.</figcaption>
</figure>
