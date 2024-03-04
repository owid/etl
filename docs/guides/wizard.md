---
status: new
---
!!! warning
    Wizard is a living project, and is constantly being improved and new features are being added. Consequently, this documentation might be slightly out of sync.

_The Wizard_ is OWID's ETL admin web app. It is an interactive [streamlit](https://streamlit.io/)-based web app that provides a user-friendly interface to manage our ETL catalog (including the creating of new ETL steps).

It was initially developed to ease the creating of ETL steps by means of templating, but it has evolved to more than that and now provides a wide range of functionalities in the ETL workflow.


Run it locally with the following command:

```bash
etlwiz
```

and then visit [localhost:8053](localhost:8053).

!!! tip "Run it from admin"
    Wizard is available from any server that bakes OWID's site (e.g. [staging servers](staging-servers.md)), including live admin page. Just look for "Wizard" in the navigation menu.

    The production version runs at [etl.owid.io/wizard](http://etl.owid.io/wizard/) (needs Tailscale).

    Note that some of the functionalities might not be enabled in a remote setting. For instance, creating steps is currently only available when running locally.

!!! tip "Use [different environments](environment.md)"

    If your Wizard session interacts with Grapher database (e.g. submit chart revisions), you can use `ENV_FILE` to connect to the appropriate server:

    ```
    ENV_FILE=.env.name etlwiz
    ```


## The different pages in Wizard
Wizard is structured into different sections, each of them grouping different pages (or apps) depending on what they do.

In the following sections we try to give a brief overview of each of the sections and the pages they contain.

### Create ETL steps
This section is dedicated to the creation of new ETL steps, including Snapshot, Meadow, Garden and Grapher steps. Additionally, Fast-Track steps can also be created using the Wizard.

In each step creation, a form is presented to the user so that they can fill in the necessary metadata fields. Based on the input, new files (e.g. python scripts, metadata YAML files, etc.) and modifications to existing ones (e.g. the DAG) are done.

After submitting each of the forms, a short guideline is shown so that the user knows what they need to do next.


### Charts
Pages to help us improve our charts (e.g. keeping them up to date). The current pages are:

- **Chart Upgrader**: Upgrade old indicators with their corresponding new versions to keep the charts up to date. You will need to (mostly) manually map "old indicators" to "new indicators". Then, the tool will create new versions of the charts with the new indicators. These versions will be kept as drafts and will need to be reviewed using the Chart Approval Tool (available from admin) before being published.
- **Chart Sync**: Synchronize charts and revisions from a server A (e.g. PR staging server) to a server B (e.g. live). This is useful when we want to migrate all the changes done in a server to the live server.

### Metadata

- **Meta Upgrader**: Upgrade v1 metadata YAML files to v2. This tool uses chatGPT to suggest the new YAML structure.
- **Meta Expert**: A GPT-powered assistant to resolve any metadata-related question.
- **Meta Playground**: A playground to test the metadata of a step. It is useful to check if the metadata is valid and to see how it will look like in a data page of an indicator.

### Others
- **Dataset Explorer**: A tool to explore the datasets in the ETL catalog. You can check the step dependancies and its metadata. If it is a Garden step, you can also perform some actions with it.
- **Entity Harmonizer**: Harmonize the entity names of a table. Mostly useful to standardise country names. An alternative to [our CLI](../etl-cli/#etl-harmonize).

## Adding new functionalities to Wizard
The code for the Wizard lives in [`apps/wizard`](https://github.com/owid/etl/tree/master/apps/wizard). It is a streamlit app, you can also run it with `streamlit run apps/wizard/app.py`.

### Adding a new page
We are trying to keep Wizard as modular as possible, so that it is easy to add new pages to it.

We encourage everyone to experiment with tools from which the team can benefit. Make sure to discuss your ideas with the rest of the team, so that you can make a good use of your time.

To add a new page, follow these steps:

1. Create a new python script, and place it under [`apps/wizard/pages`](https://github.com/owid/etl/tree/master/apps/wizard/pages). This script should be a [streamlit](https://streamlit.io/) app, and should contain the code to render the page.
2. Add an entry in the config file [`apps/wizard/config/config.yml`](https://github.com/owid/etl/blob/master/apps/wizard/config/config.yml) describing your new page. You should first decide in which section you should add your page to or create a new one. You will find more details on how to add your page in the config file itself.
