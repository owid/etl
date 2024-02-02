## Creating YAML Files

Metadata YAML files are typically stored within a garden step as `my_dataset.meta.yml`. Their content is applied **at the very end** of any ETL step. Therefore, YAML files have "the final word" on the metadata of any step. The conventional structure is as follows:

```yaml
dataset:
  update_period_days: ...

tables:
  my_table:
    variables:
      my_var_1:
        title: ...
```

To generate a metadata YAML file with pre-populated variable names for an existing garden dataset, execute:

```
poetry run etl-metadata-export data/garden/my_namespace/my_version/my_dataset
```

Check `poetry run etl-metadata-export --help` for more options.

## Handling Multi-line Strings and Whitespace

Multi-line strings are often sources of confusion. [YAML multiline](https://yaml-multiline.info/) supports two primary styles for writing them (literal and folded style), and it's up to you which option to use.

In addition, using the "strip" chomping indicator, denoted with `-`, after `|` or `>` removes whitespaces at the beginning and end of the string. **This is almost always what you want.**


### Literal style `|`
It is denoted with the `|` block style indicator. Line breaks in the YAML file are treated as line breaks.

```yaml
my_var_1:
  description_short: |-
    The first line
    and the second

    Third line after line break
```


!!! note "Note"
    This implies that lines of text in the YAML file can become very long; to be able to read them on a text editor without needing to scroll left and right, use "Word wrap" (or ++option+z++ in VS Code on Mac).

### Folded style `>`
It is denoted with the `>` block style indicator. Line breaks in the YAML file are treated like spaces; to create a line break, you need a double line break in the YAML file.

```yaml
my_var_1:
  description_short: >-
    Just a
    single line

    Second line


    Third line after line break
```

!!! note "Note"
    This avoids having lines of text that are too long in the YAML file. However, if you want to rephrase a paragraph, you may need to manually rearrange the line breaks afterwards.



## Anchors & aliases

[Anchors (&) and aliases (*)](https://support.atlassian.com/bitbucket-cloud/docs/yaml-anchors/) are a native YAML functionality and they can be used to reduce repetition.

You can define anchors anywhere on the YAML file, but typically we define a special section called `definitions` at the very top of the file, and then use aliases to refer to these definitions.

An example that reuses `attribution` and `description_key`:

```yaml
definitions:
  attribution: &attribution Fishcount (2018)
  description_key_common: &description_key_common
    - First line.
    - Second line.
  description_key_individual:
    - &third_line Third line.

tables:
  my_table:
    variables:
      my_var_1:
        description_key: *description_key_common
        presentation:
          attribution: *attribution
      my_var_2:
        description_key:
          - *description_key_common
          - *third_line
```
Note that the case of `description_key` is a bit special: You can use anchor/aliases for both the entire list of bullet points, and also individual points. We have implemented some logic so that the result is always a list of bullet points.

## Common fields for all indicators

To avoid repetition for all indicators, you can use a special section called `common:` under `definitions:`. This section sets the default metadata for indicator if there's **no specific metadata defined** in `tables:`. Using this saves you from repeating the same aliases in indicators. Note that it doesn't merge metadata, but overwrites. If you look for merge, check out `<<:` override operator.

```yaml
definitions:
  common:
    display:
      numDecimalPlaces: 1
    description_key:
      - First line.
      - Second line.
    presentation:
      grapher_config:
        selectedEntityNames:
          - Germany
          - Italy
        topic_tags:
          - Energy

tables:
  my_table:
    variables:
      my_var_1:
        # Description will be Third line
        description_key:
          - Third line.
        # Display won't be inherited from common!
        display:
          name: My var
        presentation:
          # Tag will be Internet
          topic_tags:
            - Internet
      my_var_2:
        # Description will be First line, Second line
        # Tag will be Energy
        # Display will be inherited from common
```

Specific metadata in `variables` overrides the common metadata. If you want to merge it, you can use `<<:` override operator.

```yaml
definitions:
  display: &common-display
    numDecimalPlaces: 1

tables:
  my_table:
    variables:
      my_var_1:
        display:
          name: My var
          <<: *common-display
```

You can also specify `common` for individual tables that would overwrite the `common` section under `definitions`.

```yaml
tables:
  my_table:
    common:
      display:
        numDecimalPlaces: 1

    variables:
      ...
```


## Dynamic YAML

Anchors and aliases have limitations. One of the main ones is that you cannot use it for in-line text. That's why we've added support for [dynamic-yaml](https://github.com/childsish/dynamic-yaml) which lets you write YAML files likes this:

```yaml
definitions:
  additional_info: |-
    You should also know this.

tables:
  my_table:
    variables:
      my_var_1:
        description_short: |-
          This is a description.
          { additional_info }
```

There are also special variables like `{ TODAY }` that can be used for automatically updated datasets.

## Using Jinja Templates for Advanced Cases

Even more complex metadata can be generated with [Jinja templates](https://jinja.palletsprojects.com/en/3.1.x/). This is especially useful for datasets in a long format and multiple dimensions, because Jinja lets you dynamically generate text (titles, descriptions, ...) based on the dimension names.

!!! note
    We use a slightly flavoured Jinja, where we use `<% if ... %>` and `<< var >>` instead of the defaults `{% if ... %}` and `{{ var }}`.


Find below a more complex example with dimension `conflict_type`. In this example, we use Jinja combined with dynamic YAML. Note that the dimension values are available through variables with the same name.


```yaml
definitions:
  conflict_type_estimate: |-
    <% if conflict_type == "all" %>
    The << estimate >> estimate of the number of deaths...
    <% elif conflict_type == "inter-state" %>
    ...
    <% endif %>

tables:
  ucdp:
    variables:
      number_deaths_ongoing_conflicts_high:
        description_processing: |-
          <% set estimate = "high" %>

          {definitions.conflict_type_estimate}
      number_deaths_ongoing_conflicts_low:
        description_processing: |-
          <% set estimate = "low" %>

          {definitions.conflict_type_estimate}
```

Be cautious with line breaks and trailing whitespace when utilizing templates. Despite using good defaults, you might end up experimenting a lot to get the desired result.
