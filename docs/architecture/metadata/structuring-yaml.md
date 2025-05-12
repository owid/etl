---
tags:
  - 👷 Staff
---

!!! tip "Working with YAML files in VS Code"

    Install the [YAML extension (by Red Hat)](https://marketplace.visualstudio.com/items?itemName=redhat.vscode-yaml) for VS Code to get syntax highlighting and autocompletion for YAML files. This extension will validate your files and highlight any syntax errors.

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
uv run etl metadata export data/garden/my_namespace/my_version/my_dataset
```

Check `uv run etl metadata-export --help` for more options.

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

[Anchors (&) and aliases (\*)](https://support.atlassian.com/bitbucket-cloud/docs/yaml-anchors/) are a native YAML functionality and they can be used to reduce repetition.

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

    variables: ...
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

It's also possible to do the same with Jinja `macros`. Check out section below and pick your favorite.

### Whitespaces

Line breaks and whitespaces can be tricky when using Jinja templates. We use reasonable defaults and strip whitespaces, so in most cases you should be fine with using `<%` and `%>`, but in more complex cases, you might have to experiment with
more fine grained [whitespace control](https://jinja.palletsprojects.com/en/stable/templates/#whitespace-control) using tags `<%-` and `-%>`. This is most often used in if-else blocks like this (note the `-` after `<%` for all clauses except for the first one):

```yaml
age: |-
  <% if age_group == "ALLAges" %>
  ...
  <%- elif age_group == "Age-standardized" %>
  ...
  <%- else %>
  ...
  <%- endif %>
```

An alternative to whitespace control is using the if statements in a single line, like this:

```yaml
age: |-
  <% if age_group == "ALLAges" %>
  ...<%- elif age_group == "Age-standardized" %>
  ...<%- else %>
  ...<%- endif %>
```

### Checking Metadata

The most straightforward way to check your metadata is in Admin, although that means waiting for your step to finish. There's a faster way to check your YAML file directly. Create a `playground.ipynb` notebook in the same folder as your YAML file and copy this to the first cell:

```python
import etl.grapher.helpers as gh
dim_dict = {"age_group": "YEARS0-4", "sex": "Male", "cause": "Drug use disorders"}
d = gh.render_yaml_file("ghe.meta.yml", dim_dict=dim_dict)
d["tables"]["ghe"]["variables"]["death_count"]
```

An alternative is examining `VariableMeta`

```python
from owid.catalog import Dataset
from etl import paths

ds = Dataset(paths.DATA_DIR / "garden/emissions/2025-02-12/ceds_air_pollutants")
tb = ds['ceds_air_pollutants']
tb.emissions.m.render({'pollutant': 'CO', 'sector': 'Transport'})
```

### Jinja Macros

Jinja macros could often be a good way to avoid repetition in your metadata. Define macros in field `macros:` and then import them with `{macros}`. For example:

```yaml
macros: |-
  <% macro conflict_type_estimate(conflict_type, estimate) %>
    <% if conflict_type == "all" %>
    The << estimate >> estimate of the number of deaths...
    <% elif conflict_type == "inter-state" %>
    ...
    <% endif %>
  <% endmacro %>

tables:
  ucdp:
    variables:
      number_deaths_ongoing_conflicts_high:
        description_processing: |-
          {macros}
          << conflict_type_estimate(conflict_type, "high") >>
      number_deaths_ongoing_conflicts_low:
        description_processing: |-
          {macros}
          << conflict_type_estimate(conflict_type, "low") >>
```

!!! tip "Reusing definitions across tables through shared.meta.yml"

    If you have multiple `*.meta.yml` files that share the same metadata, you can put shared `definitions:` and `macros:` into `shared.meta.yml` file. All other `*.meta.yml` files can then use them.

    For example, define a macro for formatting sex in `shared.meta.yml`:

    ```yaml
    macros: |-
      <% macro format_sex(sex) %>
        <%- if sex == "Both" -%>
        individuals
        <%- elif sex == "Male" -%>
        males
        <%- elif sex == "Female" -%>
        females
        <%- endif -%>
      <% endmacro %>
    ```

    Then, in your `*.meta.yml` files, call it as a function

    ```yaml
    tables:
      gbd_prevalence:
        variables:
          prevalence:
            description_short: |-
              The prevalence of << cause >> in << format_sex(sex) >>.
    ```


### Using Jinja in presentation.faqs

Below is a more complex example of using FAQs together with Jinja templates. Note that `definitions.my_faqs` is a multi-line string in YAML format. Use `render` method from above to debug the metadata.

```
definitions:
  gdoc_id: 1gGburArxglFdHXeTLotFW4TOOLoeRq5XW6UfAdKtaAw

  # Note that my_faqs is actually a multi-line string, not a list!
  my_faqs: |-
    - fragment_id: a-frag
      gdoc_id: "{definitions.gdoc_id}"
    - fragment_id: b-frag
      gdoc_id: "{definitions.gdoc_id}"

tables:
  gbd_prevalence:
    variables:
      prevalence:
        presentation:
          faqs:
            - fragment_id: unconditional-frag
              gdoc_id: "{definitions.gdoc_id}"
            - |-
              <% if dim_a == "A" %>
              {definitions.my_faqs}
              <% endif %>
```
