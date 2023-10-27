## I see that there are multiple fields to define the name of an indicator. When should I use what?

We currently have many fields related to an indicator's title, so you might be wondering when you should use one or the other. Find below a summary on their different use cases.

* **`indicator.title`**: This field must always be given.

    - Small datasets: it can be the publicly displayed title of the indicator in all places.
    - Big datasets:
        - These may have dimensions, and their indicators can include text fragments to communicate these breakdowns (e.g.  "Indicator name - Gender: male - Age: 10-19").
        - In such cases, `indicator.title` is mostly useful for internal searches, and a human-readable `display.name` should be given.

* **`display.name`**
    - This is our most versatile human-readable title, shown in many public places. It must be used to replace `indicator.title` when the latter has complex breakdowns.

* **`presentation.grapher_config.title`**
This should be used when a chart requires a specific title, different from the indicator's title (or `display.name`).

* **`presentation.title_public`**
This is only used for the title of a data page. It must be an excellent, human-readable title.

* **`presentation.title_variant`**
This is an additional short text that accompanies the title of the data page. It is only necessary when the indicator needs to be distinguished from others, or when we want to emphasize a special feature of the indicator, e.g. "Historical data".


In an ideal world, we could define all previous fields for all indicators, but in practice, we need to minimize our workload when creating metadata. For this reason, most of the fields are optional, and publicly displayed titles follow a hierarchy of choices.

!!! info "Title hierarchy"

    !!! warning "This is under development and might change soon, please check regularly for updates."

    In general, the hierarchy is:

    ```
    presentation.title_public > presentation.grapher_config.title > display.name > indicator.title
    ```

    The following places on our (internal/public) website will be populated using this hierarchy:

    * **Admin**
        ```
        indicator.title
        ```
    * **Charts**
        * **Table view** and **Learn more about this data** (former sources tab)
            ```
            display.name > indicator.title
            ```
        * **Chart title**
            ```
            presentation.grapher_config.title > display.name > indicator.title
            ```
    * **Data page title**
        ```
        presentation.title_public > presentation.grapher_config.title > display.name > indicator.title
        ```
