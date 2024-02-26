---
tags:
    - 👷 Staff
---

# Indicator upgrade
Consider as an example that the UN releases a new dataset on their Population estimates. In our catalog, we have an old version of it, and rely on its indicators for various charts on our site. As a consequence of this new release, we need to update our catalog with the new version, and then update all the charts that rely on the old indicators.

This process is very common in our workflow, and we have tools to help us with it. This guide will walk you through the steps to update the indicators in our catalog, and then update the charts that rely on them.

## Create the new steps
In the etl repository, create a new branch for your work.

```bash
git checkout -b data/new-dataset
```

Create all the new steps required to import the new dataset. Note that this guide only applies to those datasets that make it to the database (e.g. have Grapher steps). Other datasets are not relevant, since they won't be powering any charts.

## Add the new dataset to grapher
Once you have implemented all the steps, run the ETL to add the new dataset to the grapher database.

```bash
etl run <name> --grapher
```

With this, all the new indicators will be now available from the admin page.

## Match old indicators to new ones
!!! abstract "Chart revision"
    Charts at Our World in Data are based on a config file, which contains several configuration parameters (variable IDs in use, title, subtitle, etc.). The output of `etlwiz charts` consist of new chart configurations, based on the new dataset. This new configuration still needs some human revision.

Once a dataset is available on the Grapher database, you can create charts from its indicators.

You can select `etlwiz charts` to start the _Chart Upgrader_, which will help us match indicators from the old dataset to indicators from the new one. The tool will ask you which is the dataset being updated (old and new one) and how each indicator is mapped to a new one. The tool will guide you through the whole process of creating _chart revisions_.


!!! tip "Tips"
    - Under the expandable window "Parameters", check the "Explore indicator mappings" option to display indicator comparisons on the fly.
    - You can use the [live Wizard version for this](http://etl.owid.io/wizard/Chart%20Revision%20Baker).




Once you have finished, your chart revisions are submitted to the Grapher database, and are ready to be reviewed from there.

!!! info
    All chart suggestions are stored in the grapher database table `suggested_chart_revisions`.


## Approving new charts

Once you have successfully [created and submitted your chart revisions](../updating-charts), you should go to the admin tool "Suggested chart revisions". You can find it in the admin panel, on the right menu bar under "DATA".


In there, you will be presented with all the chart revisions, which you can either approve or reject. Note that you can filter these by the user that created them.

## Merge your changes
Once you are done, approve your pull request.

## Sync the changes to live

## Final chart approval in live
After merging your changes, you should check the charts in the live site. If everything looks good, you can approve the chart revisions in the admin tool.
