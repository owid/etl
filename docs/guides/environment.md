---
status: new
tags:
    - 👷 Staff
---

In ETL, we often have to interact with a external services (including some of ours), such as our database, OpenAI models, cloud buckets, APIs, etc.

To this end, we work with `.env` files, where we define all the required environment variables. An exmaple template is available in the root directory of the repository (see [.env.example](https://github.com/owid/etl/blob/master/.env.example)). Based on this file, staff members typically create new files:

- `.env.local` for local development.
- `.env.staging` for the staging environment.
- `.env.live` for the production environment.

To use one environment or another, you can use the prefix `ENV_FILE=.env.name` before the command. E.g. `ENV_FILE=.env.staging etl run ...` or `ENV_FILE=.env.local etlwiz charts`.

## Working with a personal staging environment
Working with a personal remote staging server is an alternative to working with a local environment. It is very easy to set up and you don't have to worry about cloning [owid-grapher](https://github.com/owid/owid-grapher).

!!! info "Set up your [personal staging server](https://www.notion.so/owid/Setting-up-a-staging-server-3e5a6591a23846ad83fba1ad6dfed4d4)"

## Commonly used environment variables

- `OPENAI_API_KEY`: OpenAI API key. Used to access OpenAI's models.
- `R2_*`: Variables used to access our cloud bucket.
- `DATA_API_ENV`: The environment where the data API is running.
- `DEBUG`: Set to `1` to enable debug mode.
- Other variables to access our database:
    - `GRAPHER_USER_ID`: The user ID of the Grapher database. Used to label user's contribution in the database.
    - `DB_USER`: The user of the database.
    - `DB_NAME`: The name of the database.
    - `DB_PASS`: The password of the database.
    - `DB_PORT`: The port of the database.
    - `DB_HOST`: The host of the database (e.g. IP).
