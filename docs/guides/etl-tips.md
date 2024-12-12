---
status: new
---

!!! warning "This is a work in progress"

    This page has been created to collect some practices when working with ETL that can be helpful for all the team to know.

    Please contribute by adding some of your tricks and tips. [Learn how to edit the documentation](../../dev/docs/).

    The content and structure of this page may change in the future.


## Interpolate values
Sometimes, you may have empty values in your dataset. In general, a solution for these cases is to use interpolation to fill those gaps based on previous and following values. In `data_helpers.misc` module, you will find the function `interpolate_table` that can be used to interpolate values in a table.

!!! note "Assumptions on the structure of `tb`"

    The function assumes that the input table has an entity column (typically for country) and a time column (year or date).

A simple call can be done as follows:

```python
from etl.data_helpers.misc import interpolate_table

tb = interpolate_table(
    tb,
    entity_col="country",
    time_col="year",
)
```

This will interpolate all the columns in the table `tb` for each country and year. It will use all years between the minimum and maximum years present in `tb`. It will use "linear" interpolation.

You can adapt the function to your needs, and perform very different kind of interpolations.

=== "Other interpolations"

    You can use any [method from pandas](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.interpolate.html).

    ```python
    tb = interpolate_table(
        tb,
        entity_col="country",
        time_col="year",
        method="quadratic"
    )
    ```

=== "Interpolate within the year range of each country"

    Sometimes, you may have different time ranges for each country. You can interpolate within the year range of each country. That is, if one country has data from 2000 to 2010, and another from 2005 to 2015, the interpolation will be done within those ranges for each country.

    ```python
    tb = interpolate_table(
        tb,
        entity_col="country",
        time_col="year",
        mode="full_range_entity",
    )
    ```

## Expand a timeseries for all years
Sometimes, you may need to expand a timeseries to include all years within a specific range, even if some years are missing in the original data. The `expand_time_column` function in the `data_helpers.misc` module can help you achieve this.


A simple call can be done as follows:

```python
from etl.data_helpers.misc import expand_time_column

tb = expand_time_column(
    tb,
    entity_col="country",
    time_col="year",
)
```

This will expand the table `tb` to include all years between the minimum and maximum years present in `tb` for each country. Missing years will be filled with NaN values.

You can adapt the function to your needs, and perform different kinds of expansions.

=== "Expand to full range for each entity"

    Expand the timeseries to include all years within the minimum and maximum years present in the data for each entity (e.g., country). Missing years will be filled with NaN values.

    ```python
    # Expand timeseries
    tb = expand_time_column(
        tb,
        entity_col="country",
        time_col="year",
        method="full_range_entity"
    )
    ```

=== "Expand to a specific range for ell entities"

    Expand the timeseries to include all years from 2000 to 2020 for all entities. Missing years will be filled with NaN values.

    ```python
    tb = expand_time_column(
        tb,
        entity_col="country",
        time_col="year",
        method="full_range",
        since_time=2000,
        until_time=2020
    )
    ```

=== "Expand with Custom Fill Value"

    Expand the timeseries to include all years within the minimum and maximum years present in the data for each entity, and fill missing years with a custom value (e.g., 0).

    ```python
    tb = expand_time_column(
        tb,
        entity_col="country",
        time_col="year",
        method="full_range_entity",
        fillna_value=0
    )
    ```

=== "Expand to Observed Years"

    Expand the timeseries to include all years that appear in the data for any entity. This ensures that all entities have rows for all observed years.

    ```python
    tb = expand_time_column(
        tb,
        entity_col="country",
        time_col="year",
        method="observed"
    )
    ```

## Deprecate code
Our codebase has lots of code. Some of it may no longer be maintained or used. To avoid confusion, it is a good practice to slowly deprecate code. This can be done by adding a deprecation warning to the code, and then removing it after a certain period of time:

```python
from deprecated import deprecated

@deprecated("This function is deprecated and will be removed in the future. Please use this other function.")
```

Make sure to point users to an alternative function or method that they can use instead.

Please deprecate function with care, and make sure to check if the function is widely used, and communicate the deprecation to the team.

## Add entity annotations to your dataset
Just add the field `display.entityAnnotationsMap` to the desired indicator.

```yaml
display:
    entityAnnotationsMap: |-
        Spain: Some annotation
        France: Another annotation
```

!!! note "Space is limited"

    The space for annotations in charts is limited. Please be mindful and keep the annotations short and to the point. 2-4 words is usually enough, ideally 2.


<figure markdown="span">
    <img src="../../assets/annotations-chart.png" alt="OWID chart with annotations" style="width:80%;">
    <figcaption>Example chart with entity annotations. Note that the space for annotations.</figcaption>
</figure>
