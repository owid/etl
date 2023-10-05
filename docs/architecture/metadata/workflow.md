## "Live" reloading of a Data page

When you work on a YAML file for a Data page, you want to see the changes in your browser as soon as possible. Running `etl ... --grapher` after each change is tedious. Instead, you can use a few tricks to make the process faster.

Let's take the step `grapher/gcp/2023-07-10/global_carbon_budget` as an example. I'm updating its YAML file `global_carbon_budget.meta.yml` in the grapher channel and work on its [Data page](http://staging-site-mojmir/admin/datapage-preview/738081) on my staging server.

1. First add `DEBUG=1` to your `.env.myname` (and all other `.env.*` files you use). This speeds up ETL.

2. Run the ETL command with `--watch` to watch for any changes to the file and rerunning step automatically.

3. The dataset has lots of indicators, but we can filter them to just the variable of interest with `GRAPHER_FILTER=consumption_emissions_per_capita`. This is not necessary for small datasets.

4. (Optional) Adding `--only` might speed things up even further as it won't check dependencies.

In the end, the command looks like this:

```bash
ENV=.env.myname GRAPHER_FILTER=consumption_emissions_per_capita etl grapher/gcp/2023-07-10/global_carbon_budget --grapher --watch --only
```

Now you can edit the YAML file, switch to [browser](http://staging-site-mojmir/admin/datapage-preview/738081) and **refresh the page** to see the changes. Changes should be visible within a few seconds.

!!! info
    If you're changing the YAML file in the **garden channel** and it takes a long time to run, refresh speed will be limited by the time it takes to run the step. In this case, you might be better off developing the YAML file in the **grapher channel** and then merging it with the garden channel.
