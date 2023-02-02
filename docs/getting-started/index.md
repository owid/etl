# Getting started

The ETL can be used by OWID staff or the general public to build a copy of our data catalog. Here's how to get set up.

## Getting set up
The ETL is supported and regularly run on Linux, MacOS and Windows via WSL.

### Install dependencies


You will need Python 3.9+, basic build tools, and MySQL client libraries:

=== "MacOS"

    We recommend using [Homebrew](https://brew.sh/) to install dependencies.

    Ensure you have XCode command line tools:

    ```bash
    xcode-select --install
    ```

    Then install Python 3.9+ and MySQL client:

    ```
    brew install python mysql-client poetry
    ```
    
    You then need to inform Python where to find MySQL by adding some lines to your `~/.zshrc` file. Run `brew info mysql-client` to see what's needed.
    
    For example, on an M1/M2 Mac where Homebrew installs to `/opt/homebrew`, you add:
    
    ```
    export PATH="/opt/homebrew/opt/mysql-client/bin:$PATH"
    export LDFLAGS="-L/opt/homebrew/opt/mysql-client/lib"
    export CPPFLAGS="-I/opt/homebrew/opt/mysql-client/include"
    ```
    
    On an Intel Mac, the paths will be slightly different.
    
    Finally, check that you have the correct version of Python as your default:

    ```bash
    which python3
    ```

    It should say something like `/usr/local/bin/python3` or `/opt/homebrew/bin/python3`. If not, you will have to change the `PATH` variable in your shell profile (e.g. `~/.bash_profile` or `~/.zshrc`).

=== "Ubuntu 22.04"

    You can install most things you need with `apt`:

    ```bash
    sudo apt install python3-dev python3-virtualenv python3-setuptools mysql-client
    ```

    However, the version of Poetry that ships with Ubuntu is too old, so we need to install a more recent version.

    The [recommended method](https://python-poetry.org/docs/#installation) is to run:

    ```bash
    curl -sSL https://install.python-poetry.org | python3 -
    ```

=== "Windows"

    You will need to [install WSL2](https://learn.microsoft.com/en-us/windows/wsl/install) to get started.

    You should use [Ubuntu 22.04](https://apps.microsoft.com/store/detail/ubuntu-22041-lts/9PN20MSR04DW?hl=en-au&gl=au&rtc=1) as your Linux distribution.

    Then, enter your Linux console and follow the instructions for Ubuntu 22.04.

??? "Extra config for staff"

    OWID staff who want to add steps to the ETL will also need an `~/.aws/config` file configured, so that you can use snapshots.

    The file itself will look like this:

    ```
    [default]
    aws_access_key_id = <access_key>
    aws_secret_access_key = <secret_key>
    ```

    Please ask someone in the `#data-architecture` Slack channel to help you get set up.

## Check your environment

You can get started by using ``make`` to see available commands. The best way to check if your environment is healthy is to run:

```bash
make test
```

It will clone two submodules in the ``vendor/`` folder, run ``poetry install``, and then run all CI checks.

If ``make test`` succeeds, then you should be able to build any dataset you like, including the entire catalog. If it fails, feel free to raise a `Github issue <https://github.com/owid/etl/issues>`_, or OWID staff can also ask using the ``#tech-issues`` Slack channel.

## Building datasets

Before continuing, activate your Python virtual environment by running:

```bash
$ source .venv/bin/activate
```

### Dry-runs

Every step in the dag has a URI. For example, Our World In Data's population density dataset has the URI:

```
data://garden/ggdc/2020-10-01/ggdc_maddison
```

We can see what steps would be executed to build it by running::

```bash
$ etl --dry-run data://garden/ggdc/2020-10-01/ggdc_maddison
Detecting which steps need rebuilding...
Running 2 steps:
1. snapshot://ggdc/2020-10-01/ggdc_maddison.xlsx...
2. data://garden/ggdc/2020-10-01/ggdc_maddison...
```

The first step is a `snapshot://` step, which when run will download an upstream snapshot of this dataset to the ``~/.owid/walden`` folder.

The second step is a `data://` step, which will generate a local dataset in the `data/` folder of the top-level `etl/` folder.

Observe that we can also skip the full path of the step, in which case it will do a regex match against all available steps:

```
$ etl --dry-run ggdc_maddison
```

Now let's build the dataset, by removing the ``--dry-run`` option:

```
$ etl data://garden/ggdc/2020-10-01/ggdc_maddison
Detecting which steps need rebuilding...
Running 2 steps:
1. snapshot://ggdc/2020-10-01/ggdc_maddison.xlsx...
OK (2s)

2. data://garden/ggdc/2020-10-01/ggdc_maddison...
OK (4s)
```

Let's confirm that the dataset was built locally:

```
$ ls data/garden/ggdc/2020-10-01/ggdc_maddison/
index.json
maddison_gdp.feather
maddison_gdp.meta.json
maddison_gdp.parquet
```

Several files got built for the dataset. The first is `index.json` which gives metadata about the whole dataset. The remaining three files all represent a single data table, which is saved in both Feather and Parquet formats.

## Consuming data

Now that our `data/` folder has a table built, we can try reading it.  Let's run `python` and use Pandas:

```pycon
>>> import pandas as pd
>>> df = pd.read_feather('data/garden/ggdc/2020-10-01/ggdc_maddison/maddison_gdp.feather')
>>> df.head()
    country  year  gdp_per_capita  population           gdp
0  Afghanistan  1820             NaN   3280000.0           NaN
1  Afghanistan  1870             NaN   4207000.0           NaN
2  Afghanistan  1913             NaN   5730000.0           NaN
3  Afghanistan  1950          1156.0   8150000.0  9.421400e+09
4  Afghanistan  1951          1170.0   8284000.0  9.692280e+09
```

We can see that this dataset provides three indicators (`gdp`, `population`, and `gdp_per_capita`), reported by country and year.

All tables generated by the ETL can also be read and written using a wrapper around Pandas, the `Table` class. If we read the table using that, it will also pick up the metadata that was in the `.meta.json` file.

```pycon
>>> from owid.catalog import Table
>>> t = Table.read('data/garden/ggdc/2020-10-01/ggdc_maddison/maddison_gdp.feather')
>>> t.head()
                gdp_per_capita  population           gdp
country     year
Afghanistan 1820             NaN   3280000.0           NaN
            1870             NaN   4207000.0           NaN
            1913             NaN   5730000.0           NaN
            1950          1156.0   8150000.0  9.421400e+09
            1951          1170.0   8284000.0  9.692280e+09
```

In this case, we can see that it understood that `country` and `year` columns were the primary key for this table, and put them in the index.
