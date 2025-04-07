# Global food explorer

Everything in this folder is used to generate the massive spreadsheet config for the global food explorer.

## Building the explorer

Firstly, ensure the `VERSION` and `YEAR` variables defined in `global-food-explorer.py` are the correct ones, aligned with the latest ETL version of the food explorer step.

You need Python 3.9 with `poetry` installed, then run `make`. It will generate the top-level explorer config `global-food.explorer.tsv`, if it's out of date.

## Files

### `global-food-explorer.py`

This is the Python script that combines all the input files into a single `.explorer.tsv` file.
Set it up using `poetry install` and run it using `poetry run python global-food-explorer.py`.

There is also a GitHub action set up that will automatically generate the explorer config for every Pull Request or push to `staging` or `master`.

### `foods.csv`

This file defines all food products that will be available in the explorer.
We have the following columns:

| name       | description                                                                                                                                                                                                       |
| :--------- | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `slug`     | The slug needs to match the filename of the data file. E.g. if the data file is called `meat_rabbit.csv`, then the slug should be `meat_rabbit`.                                                                  |
| `dropdown` | This is what is displayed in the food dropdown of the explorer.                                                                                                                                                   |
| `singular` | The singular form of the food product, as used in titles. It should be in title case (i.e. written as seen in the beginning of a sentence) and should fit in a sentence such as `Land used for [...] production`. |
| `plural`   | The plural form of the food product, as used in titles. It should be in title case (i.e. written as seen in the beginning of a sentence) and should fit in a sentence such as `Domestic supply of [...]`.         |
| `_tags`    | The tags of this food product. This entry determines which views to show for this food product. See [Tags](#Tags) below.                                                                                          |

### `views-per-food.csv`

This file determines which views will be available for different food products.
It follows the same syntax as the `graphers` section of an explorer spreadsheet.
Additionally, the `title` and `subtitle` columns support the following placeholders for the particular food name:

- `${food_singular}` will be replaced by the singular version of the word as written in `foods.csv`, i.e. starting with an uppercase letter.
  - Example: `${food_singular} production` → `Apple production`
- `${food_singular_lower}` will be replaced by the lowercase version of the singular given in `foods.csv`.
  - Example: `Land used for ${food_singular_lower} production` → `Land used for apple production`
- `${food_plural}` will be replaced by the plural version of the word as written in `foods.csv`, i.e. starting with an uppercase letter.
  - Example: `${food_plural} used for animal feed` → `Apples used for animal feed`
- `${food_plural_lower}` will be replaced by the lowercase version of the plural given in `foods.csv`.
  - Example: `Domestic supply of ${food_plural_lower}` → `Domestic supply of apples`

Additionally, there is a special `_tags` column that will not be part of the output `.explorer.tsv` file.
It can contain a comma-separated list of [Tags](#Tags), specifying that the view will be available for all food products with this tag.

### `global-food-explorer.template.tsv`

In this template file, the boilerplate gluing the whole explorer spreadsheet together is defined. That includes, among others:

- The explorer title and subtitle
- The default country selection
- The column definitions of the data files, including source name, unit, etc.
- Three special placeholders: `$graphers_tsv`, `table_defs`, `food_slugs`.
  It's probably best not to touch them unless you know exactly what you're doing :)

## Tags

Both the `foods.csv` and `views-per-food.csv` files have a special `_tags` column.
In this, a comma-separated list of tags can be given that specifies which view will be available for which food product.
Let's see how this works based on an example:

`foods.csv`

| \_tags             | slug        |
| :----------------- | :---------- |
| qcl,fbsc,crop      | apples      |
| qcl,animal-product | meat_rabbit |
| fbsc,crop          | barley      |

`views_per_food.csv`

| \_tags         | title             |
| :------------- | :---------------- |
| qcl            | Production        |
| crop           | Yield [t/ha]      |
| crop           | Land use          |
| animal-product | Yield [kg/animal] |
| fbsc           | Imports           |

With this configuration, the following metrics would be available for the different foods:

- apples: Production, Yield [t/ha], Land use, Imports
- meat_rabbit: Production, Yield [kg/animal]
- barley: Yield [t/ha], Land use, Imports

Through the use of these tags, certain views can be enabled and disabled on a per-food basis.
