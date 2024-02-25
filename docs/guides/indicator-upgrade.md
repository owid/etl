---
tags:
    - 👷 Staff
---

# Indicator upgrade
Consider as an example that the UN releases a new dataset on their Population estimates. In our catalog, we have an old version of it, and rely on its indicators for various charts on our site. As a consequence of this new release, we need to update our catalog with the new dataset, and then update all the charts that rely on the old indicators.

This process is very common in our workflow, and we have tools to help us with it. This guide will walk you through the steps to update the indicators in our catalog, and then update the charts that rely on them.

## Create a new branch

## Updating the dataset
- Create PR
- Work on your new import using Wizard?
- Edit scripts, make sure it works and push to Grapher

## Mapping old indicators to new ones

Once a dataset is available on the Grapher database, you can create charts from its indicators. In some cases, the dataset is a newer version from an already existing one. In such cases, we have to update the charts that rely on indicators from the old dataset.

To this end, we use `etlwiz charts`. This tool will guide you through the whole process of creating _chart revisions_.

!!! abstract "Chart revision"
    Charts at Our World in Data are based on a config file, which contains several configuration parameters (variable IDs in use, title, subtitle, etc.). The `etlwiz charts` creates a new configuration, based on the new dataset. This new configuration still needs some human revision. That is, needs revision.

To start creating your chart revisions, run


```
poetry run etlwiz charts
```

This should open the webapp in your browser.

To generate the chart revisions, the tool needs to know how indicators in the old dataset map to the ones in the new one. To this end, the tool will ask you which is the dataset being updated (old and new one) and how each indicator is mapped.

!!! tip "Explore mode"
    Under the expandable window "Parameters", check the "Explore indicator mappings" option to display indicator comparisons on the fly.


Once you have finished, your chart revisions are submitted to the Grapher database, and are ready to be [reviewed from the admin](../reviewing-charts).

!!! info
    All chart suggestions are stored in the grapher database table `suggested_chart_revisions`.


## Approving new charts

Once you have successfully [created and submitted your chart revisions](../updating-charts), you should go to the admin tool "Suggested chart revisions". You can find it in the admin panel, on the right menu bar under "DATA".


In there, you will be presented with all the chart revisions, which you can either approve or reject. Note that you can filter these by the user that created them.

## Merge your changes
Once you are done, approve your pull request.

## Final chart approval in live
After merging your changes, you should check the charts in the live site. If everything looks good, you can approve the chart revisions in the admin tool.
