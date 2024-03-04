---
status: new
tags:
    - 👷 Staff
---
!!! tip "`STAGING` over `ENV_FILE`"
    Using `STAGING` variable is __recommended way__ over using `ENV_FILE` when working with staging servers. It is easier and more secure to use than `ENV_FILE`.

    Hence, we do not recommend using `ENV_FILE` unless you are aware of what it comprises.


In ETL, we often have to interact with a external services (including some of ours), such as our database, OpenAI models, cloud buckets, APIs, etc.

To this end, we work with `.env` files, where we define all the required environment variables. An example template is available in the root directory of the repository (see [.env.example](https://github.com/owid/etl/blob/master/.env.example)). Based on this file, staff members create their own `.env` file.


!!! note "Which environment is used?"
    By default, your commands will load all the environment variables defined in `.env`. In case you want to use another file, you can use the prefix `ENV_FILE=.env.name` before the command. E.g. `ENV_FILE=.env.name etl run ...` or `ENV_FILE=.env.name etlwiz charts`.

## Working with staging environments

To use staging servers, use the environment variable `STAGING` to select which staging server to use.

Personal staging servers use usernames, and PR staging servers use the branch name. For example,

```
STAGING=yourname etl run <short_name> --grapher
```

or

```
STAGING=feature-123 etl run <short_name> --grapher
```

### Personal staging environment
Working with a personal remote staging server is an alternative to working with a local environment. It is very easy to set up and you don't have to worry about cloning [owid-grapher](https://github.com/owid/owid-grapher).

!!! info "Set up and refresh your [personal staging server](https://www.notion.so/owid/Setting-up-a-staging-server-3e5a6591a23846ad83fba1ad6dfed4d4)"

Note that personal staging servers go rapidly out of sync (contrary to PR staging servers), and are not meant to be used for long-term development but rather for quick tests.

### PR staging environment
Whenever you create a pull request in the etl repository, a dedicated staging server is automatically created. This server runs the OWID admin site and database, and includes all the changes from your PR (e.g. new datasets). This allows you to test your changes before they are merged into the live site.

These servers are useful to share your changes with other members of the team, and, for instance, to [create chart revisions][indicator-upgrade#match-old-indicators-to-new-ones].

!!! info "Learn more about [PR staging servers](../staging-servers)"

## Commonly used environment variables
`.env` files can have some of the following variables defined in them:

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
