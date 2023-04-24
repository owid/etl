The ETL can be used by OWID staff or the general public to build a copy of our data catalog. It is supported and regularly run on Linux, MacOS and Windows via WSL. Here's how to get set up.

!!! warning

    This documentation is still under construction. Please [report any issue](https://github.com/owid/etl/issues/new?assignees=&labels=&template=docs-issue---.md) that you may find so we can keep improving our docs.


*[WSL]: Windows Subsystem for Linux

## Install dependencies

You will need Python 3.9+, basic build tools, and MySQL client libraries.

=== "MacOS"

    !!! tip

        We recommend using [Homebrew](https://brew.sh/) to install dependencies.

    Ensure you have XCode command line tools:

    ```bash
    xcode-select --install
    ```

    Then install Python 3.9+, MySQL client and [poetry](https://python-poetry.org/). Poetry is our preferred python packaging and dependency management tool.

    ```bash
    brew install python mysql-client poetry
    ```

    You then need to inform Python where to find MySQL by adding some lines to your `~/.zshrc` file (or `~/.bash_profile`, depends on your shell). Run `brew info mysql-client` to see what's needed. For example, on an M1/M2 Mac where Homebrew installs to `/opt/homebrew`, you would need to add:

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

## Clone the project
First of all, you need to have the [ETL project](https://github.com/owid/etl) in your working environment. Run:

```bash
git clone https://github.com/owid/etl.git
```

Along with various directories and files, the project also has two submodules in the `vendor/` folder: [owid-catalog-py](https://github.com/owid/owid-catalog-py) and [walden](https://github.com/owid/walden) (deprecated), both in-house developed libraries, which simplify the interaction with our datasets.

## Check your environment

You can get started by using `make` to see available commands. Note that to run all `make` commands you should be in the project folder (as it contains the `Makefile`).

```bash
make help
```

The best way to check if your environment is healthy is to run:

```bash
make test
```

This will update the two submodules in the `vendor/` folder, install the project, and then run all CI checks.

If `make test` succeeds, then you should be able to build any dataset you like, including the entire catalog. If it fails, please raise a [Github issue](https://github.com/owid/etl/issues) (if OWID staff, you can also ask using the `#tech-issues` Slack channel).

## Project folder
The project has multiple folders and directories. Let's try to make sense of them.

Start by listing all the files in the project:

```bash
cd etl/
ls
```

This will list all the folders and directories in the project. Find a brief explanation on the most relevant ones in the following table.


| Folder (or file)      | Description                          |
| ----------- | ------------------------------------ |
| `etl/`       | This is home to our ETL library. This is where all the recipes to generate our datasets live. |
| `snapshots/`       | This is the entry point to ETL. This folder contains metadata and code to get external data and import it to our pipeline. |
| `dag/`    | Contains the dataset dependencies. That is, if `dataset A` needs `dataset B` to be up to date, this should be listed here. |
| `walkthrough/`    | High-level tool to help create dataset recipes. |
| `fasttrack/`    | Very high-level tool to create add dataset recipes. This tool is used in instanced where time is an issue. |
| `data/`    | When you run the recipe code for a dataset, the dataset will be created under this directory. Note that not all the content from this directory is added to git. |
| `schemas/`    | Metadata schemas for ETL datasets. |
| `scripts/`    | Various scripts. |
| `tests/`    | ETL library tests. |
| `vendor/`    | Dependencies of other OWID git projects. |
| `docs/`, `.readthedocs.yaml`, `mkdocs.yml`    | Project documentation config files and directory. |
| `.dvc/`, `.dvcignore`       | DVC config folder and file.  |
| `Makefile`, `default.mk`    | `make`-related files. |

*[DVC]: Data Version Control
