# Creating chart revisions

!!! warning "This tutorial might be partial or incomplete. Please check with the team if you have questions."

Once a dataset is available on the Grapher database, you can create charts from its indicators. In some cases, the dataset is a newer version from an already existing one. In such cases, we have to update the charts that rely on indicators from the old dataset.

To this end, we use `etl-wizard charts`. This tool will guide you through the whole process of creating _chart revisions_.

!!! abstract "Chart revision"
    Charts at Our World in Data are based on a config file, which contains several configuration parameters (variable IDs in use, title, subtitle, etc.). The `etl-wizard charts` creates a new configuration, based on the new dataset. This new configuration still needs some human revision. That is, needs revision.

To start creating your chart revisions, run


```
poetry run etl-wizard charts
```

This should open the webapp in your browser.

To generate the chart revisions, the tool needs to know how indicators in the old dataset map to the ones in the new one. To this end, the tool will ask you which is the dataset being updated (old and new one) and how each indicator is mapped.

!!! tip "Explore mode"
    Under the expandable window "Parameters", check the "Explore indicator mappings" option to display indicator comparisons on the fly.


Once you have finished, your chart revisions are submitted to the Grapher database, and are ready to be [reviewed from the admin](../reviewing-charts).

!!! info
    All chart suggestions are stored in the grapher database table `suggested_chart_revisions`.
