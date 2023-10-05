## Creating YAML files

Metadata YAML files are most commonly saved in a garden step with name `my_dataset.meta.yml`. The typical structure looks like this:

```yaml
dataset:
  update_period_days: ...

tables:
  my_table:
    variables:
      my_var_1:
        title: ...
```

If you already have a garden dataset, you can generate the file automatically with pre-filled variables names with

```
poetry run etl-metadata-export data/garden/my_namespace/my_version/my_dataset -o etl/steps/data/garden/my_namespace/my_version/my_dataset.meta.yml
```

## Multi-line strings and whitespaces

Multi-line strings are a common source of confusion. There are two ways to write them:

```yaml
my_var_1:
  description: |-
    The first line
    and the second

    Third line after line break
```

```yaml
my_var_1:
  description: >-
    Just a
    single line

    Second line


    Third line after line break
```

It's up to you whether you want to fit the entire text on the single line and use `>-` or use `|-` and break the lines.

Using `-` after `|` or `>` removes whitespaces at the beginning and end of the string. This is almost always what you want.


## Anchors & aliases

[Anchors and aliases](https://support.atlassian.com/bitbucket-cloud/docs/yaml-anchors/) are a native YAML functionality and they can be used to reduce repetition.

Typically we define a special section called `definitions:` at the very top of the file and then we use aliases to refer to these definitions.

An example that reuses `attribution` and `description_key`

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


## Common fields for all indicators

!!! warning "In progress."

To avoid repetition for all indicators, you can use a special section called `common:` under `definitions:`. This section is then merged with sections of all indicators. Using this saves you from repeating the same aliases in indicators.

```yaml
definitions:
  common:
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
        # Final description will be First, Second, Third line
        description_key:
          - Third line.
        presentation:
          # Final tags will be Internet, Energy
          topic_tags:
            - Internet
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
        description: |-
          This is a description.
          { additional_info }
```

There are also special variables like `{ TODAY }` that can be used for automatically updated datasets.


## Jinja templates

Even more complex metadata can be generated with [Jinja templates](https://jinja.palletsprojects.com/en/3.1.x/). This is especially useful for datasets in a long format, because Jinja lets you dynamically generate text (titles, descriptions, ...) based on dimension names.

We use tags `<% if ... %>` and `<< var >>` instead of default `{% if ... %}` and `{{ var }}`.

Dimension values are available through variables with the same name.

Here is a more complex example with dimension `conflict_type`:

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
        description: |-
          <% set estimate = "high" %>

          {definitions.conflict_type_estimate}
      number_deaths_ongoing_conflicts_low:
        description: |-
          <% set estimate = "low" %>

          {definitions.conflict_type_estimate}
```

Line breaks and trailing whitespaces can be tricky when using templates. Despite using good defaults, you might end up experimenting a lot to get the desired result.
