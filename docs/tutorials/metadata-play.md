# Metadata in data pages
To better understand how the different metadata fields are mapped to the different elements in a data page, we have created a very simple app.

Simply execute

```shell
poetry run etl-metaplay
```

!!! note "Use the correct environment variables"

    Make sure to run this with the appropriate environment variables set (you need access to the database). This works best with your staging environment (accessible via Tailscale).

    You can define custom environment variables in the file `.env.staging` and then run:

    ```shell
    ENV=.env.staging poetry run etl-metaplay
    ```


The previous command will launch the web app.


![](../assets/metaplay-short.gif)


This app uses a fictional dataset called "dummy", to build the data page for its indicator `dummy_variable`. In the app, you can see and edit the metadata for the origin (in Snapshot) and the metadata for the indicator (in Garden). In the background, this will run the necessary ETL steps to ingest this indicator into Grapher.

