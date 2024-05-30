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

!!! tip

    Set `STAGING=1` in your `.env` to automatically detect the staging server from the branch name.


### Personal staging environment
Working with a personal remote staging server is an alternative to working with a local environment. It is very easy to set up and you don't have to worry about cloning [owid-grapher](https://github.com/owid/owid-grapher).

To set it up, follow these steps:

1. Go to `etl` repository and create a new branch with your name `git checkout -b yourname`
2. Create an empty commit with `git commit -m "🚧 automated staging server: Yourname" --allow-empty`
3. Push it to `etl` with `git push origin yourname` and create a pull request with the name “🚧 automated staging server - yourname”. Convert PR to a draft (an example) and add label `staging` to the pull request

After approximately 5 minutes, your server will be ready, and you’ll be able to access it on the URLs below

```
Staging server staging-site-yourname created
- Login: ssh owid@staging-site-yourname
- Admin: http://staging-site-yourname/admin/login (admin@example.com/admin)
- Site: http://staging-site-yourname/
- MySQL: mysql -h staging-site-yourname -u owid --port 3306 -D owid
```

If you want to run ETL against a staging server, use `STAGING=yourname` flag. For instance
```
STAGING=yourname etl ... --grapher
```

Note that personal staging servers go rapidly out of sync (contrary to PR staging servers), and are not meant to be used for long-term development but rather for quick tests. To update your server to the most recent changes, rebase and push back to your branch
```
git fetch && git rebase origin/master && git push -f
```

### PR staging environment
Whenever you create a pull request in the etl repository, a dedicated staging server is automatically created. This server runs the OWID admin site and database, and includes all the changes from your PR (e.g. new datasets). This allows you to test your changes before they are merged into the live site.

These servers are useful to share your changes with other members of the team, and, for instance, to [create chart revisions](indicator-upgrade#match-old-indicators-to-new-ones).

!!! info "Learn more about [PR staging servers](../staging-servers)"

### Useful Operations with Staging Servers

#### Refresh the Staging Server MySQL Database

To refresh the MySQL database on the staging server, use the following command:

```sh
ssh owid@staging-site-yourname 'cd owid-grapher && make refresh'
```

**Note:** This will **delete all data on staging** and replace it with the latest data from the production database.

#### Backup the Staging Server

To back up the staging server, run:

```sh
owid-lxc copy staging-site-yourname backup-staging-site-yourname
```

The backup will not be automatically destroyed.

#### Factory Reset the Staging Server

To "factory reset" the staging server, first destroy it with:

```sh
owid-lxc destroy staging-site-yourname
```

Then push a new commit to recreate it.

#### Explorers on Staging Servers

To test explorers on data from your staging server, change their URL to:

[http://staging-site-mybranch:8881/explorers/namespace/version/dataset/table.csv](http://staging-site-mojmir:8881/explorers/who/latest/flu/flu.csv)

for instance

http://staging-site-mojmir:8881/explorers/who/latest/flu/flu.csv


## Commonly used environment variables
`.env` files can have some of the following variables defined in them:

- `OPENAI_API_KEY`: OpenAI API key. Used to access OpenAI's models.
- `R2_*`: Variables used to access our cloud bucket.
- `DATA_API_ENV`: The environment where the data API is running.
- `DEBUG`: Set to `1` to enable debug mode (faster local development).
- `STAGING`: Set to `1` to automatically detect STAGING from branch name or to name of the staging server.
- Other variables to access our database:
    - `GRAPHER_USER_ID`: The user ID of the Grapher database. Used to label user's contribution in the database.
    - `DB_USER`: The user of the database.
    - `DB_NAME`: The name of the database.
    - `DB_PASS`: The password of the database.
    - `DB_PORT`: The port of the database.
    - `DB_HOST`: The host of the database (e.g. IP).
