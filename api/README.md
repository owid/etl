# ETL API

API for ETL lets you edit metadata for indicators by creating `*.meta.override.yml` file which overrides
YAML file at a grapher level. With this file the API then executes:

1. Slow track - commit and push to ETL which fully rebuilds the step (> minutes)
2. Fast track - run `etl step --grapher` to update MySQL and R2 file (typically < 1 minute)
2. Super Fast track - update indicator R2 file so that Admin shows updated values (enabling live preview of a chart or data page)

Since Admin & our site is powered by R2 JSON files, we don't have to deploy the site.

## Limitations

It doesn't work for old non-ETL datasets and older steps that don't use `create_dataset` function. If user tries to use it for those datasets, API returns 4xx errors (together with other validation errors).

## Observability

If env `SLACK_API_TOKEN` is set, it'll send all requests & responses to a slack channel (with highlighted warnings). There's also a bugsnag integration for error monitoring.

## Instructions

1. Run `make api` to start ETL API on http://localhost:8000/v1/indicators
2. Enable "Slow track" by setting `ETL_API_COMMIT=1` which commits the changed file and pushes it to origin.

## Sample request

The following request edits indicator with path `grapher/biodiversity/2023-01-11/cherry_blossom/cherry_blossom#full_flowering_date`(and id `540251`). It sets decimal places, changes its title and
triggers ETL rebuild (without committing the code).

```
echo '{
  "catalogPath": "grapher/biodiversity/2023-01-11/cherry_blossom/cherry_blossom#full_flowering_date",
  "indicator": {
    "name": "My new name",
    "display": {
      "numDecimalPlaces": 2
    }
  },
  "dataApiUrl": "https://api-staging.owid.io/mojmir/v1/indicators/",
  "triggerETL": true,
  "dryRun": false
}' | http PUT http://127.0.0.1:8000/v1/indicators

```

`dataApiUrl` points to the Data API that is used by Admin. If ETL API uses different Data API, it'll
raise an error (to prevent confusion when querying the wrong ETL API).
