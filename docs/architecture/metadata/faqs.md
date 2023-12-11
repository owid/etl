## I see that there are multiple fields to define the name of an indicator. When should I use what?

We currently have many fields related to an indicator's title, so you might be wondering when you should use one or the other. Find below a summary on their different use cases.

* **`indicator.title`**: This field must always be given.

    - Small datasets: it can be the publicly displayed title of the indicator in all places.
    - Big datasets:
        - These may have dimensions, and their indicators can include text fragments to communicate these breakdowns (e.g.  "Indicator name - Gender: male - Age: 10-19").
        - In such cases, `indicator.title` is mostly useful for internal searches, and a human-readable `presentation.title_public` should be given.

* **`presentation.title_public`**
This is our most versatile human-readable title, shown in many public places. It must be used to replace `indicator.title` when the latter has complex breakdowns. It must be an excellent, human-readable title.

* **`presentation.title_variant`**
This is an additional short text that accompanies an indicator's title. It is only necessary when the indicator needs to be distinguished from others, or when we want to emphasize a special feature of the indicator, e.g. "Historical data".

* **`presentation.grapher_config.title`**
This should be used when a chart requires a specific title, different from the indicator's title (or `presentation.title_public`).

* **`display.name`**
    - This should only be used to customize the indicator's title in the legend of a chart.
    - For backwards compatibility, `display.name` also replaces the indicator's title in other public places. Therefore, whenever `display.name` is defined, `presentation.title_public` should also be defined.

In an ideal world, we could define all previous fields for all indicators, but in practice, we need to minimize our workload when creating metadata. For this reason, most of the fields are optional, and publicly displayed titles follow a hierarchy of choices.

!!! info "Title hierarchy"

    !!! warning "This is under development and might change soon, please check regularly for updates."

    The following places on our (internal/public) website will be populated using this hierarchy:

    * **About this data**
        * Currently: `[display.name > title] - [description_short]`
        * Soon: `[title_public > grapher_config.title  > display.name > title] - [title_variant] - [attribution_short] - [description_short]`
    * **Table tab**
        * Currently: `[display.name > title]`
        * Soon: `[title_public > display.name > title] - [title_variant] - [attribution_short]`
    * **Heading of a 1-indicator chart**
        * Currently: `[grapher_config.title > display.name > title] - [grapher_config.subtitle > description_short]`
        * Soon: `[grapher_config.title > title_public > display.name > title] - [grapher_config.subtitle > description_short]`
    * **Legend in a chart**
        * Currently: `[display.name > title]`
        * Soon: `[display.name > title_public > title]`
    * **Data page title**
        * `[title_public > grapher_config.title > display.name > title] - [attribution_short] - [title_variant]`
    * **Search result**
        * Currently: `[title_public > grapher_config.title > display.name > title] - [title_variant]`
        * Soon: `[title_public > grapher_config.title > display.name > title] - [title_variant] - [attribution_short] - [description_short]`
    * **Admin page**:
        * `[title]`
