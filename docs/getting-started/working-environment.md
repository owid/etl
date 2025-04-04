# Installation
The ETL can be used by OWID staff or the general public to build a copy of our data catalog. It is supported and regularly run on Linux, MacOS and Windows via WSL. Here's how to get set up.

!!! warning

    This documentation is still under construction. Please [report any issue](https://github.com/owid/etl/issues/new?assignees=&labels=&template=docs-issue---.md) that you may find so we can keep improving our docs.


*[WSL]: Windows Subsystem for Linux

## Install dependencies

You will need Python 3.10+, basic build tools, and MySQL client libraries.

=== "MacOS"

    !!! tip

        We recommend using [Homebrew](https://brew.sh/) to install dependencies.

    Ensure you have XCode command line tools:

    ```bash
    xcode-select --install
    ```

    Then install Python 3.9+ and MySQL client and [UV](https://docs.astral.sh/uv/). UV is our preferred python packaging and dependency management tool.

    ```bash
    brew install python mysql-client uv pkg-config
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
    sudo apt install python3-dev python3-virtualenv python3-setuptools mysql-client libmysqlclient-dev
    ```

    Then install UV package manager with

    ```bash
    curl -LsSf https://astral.sh/uv/install.sh | sh
    ```

    or

    ```bash
    pip install uv
    ```


=== "Windows"

    You will need to [install WSL2](https://learn.microsoft.com/en-us/windows/wsl/install) to get started.

    You should use [Ubuntu 22.04](https://apps.microsoft.com/store/detail/ubuntu-22041-lts/9PN20MSR04DW?hl=en-au&gl=au&rtc=1) as your Linux distribution.

    Then, enter your Linux console and follow the instructions for Ubuntu 22.04.

??? "Extra config for staff"

    OWID staff who want to upsert data from ETL to grapher database will also need access to Cloudflare R2.

    First start with installing `rclone`

    ```bash
    brew rclone
    ```

    Then configure its config with `code ~/.config/rclone/rclone.conf`. You should get your personal R2 keys
    `r2_access_key_id` and `r2_secret_access_key` and replace them in the config file.

    ```bash
    [owid-r2]
    type = s3
    provider = Cloudflare
    env_auth = true
    access_key_id = r2_access_key_id
    secret_access_key = r2_secret_access_key
    region = auto
    endpoint = https://078fcdfed9955087315dd86792e71a7e.r2.cloudflarestorage.com

    [r2]
    type = alias
    remote = owid-r2:
    ```


## Install pyenv

!!! tip

    `pyenv` is not crucial now after switching to `uv` as a package manager. However, it is still recommended to use it to manage your Python versions.

Even though it's not compulsory, it is **highly recommended** to install [pyenv](https://github.com/pyenv/pyenv#installation) to manage your Python versions. This will allow you to have multiple Python versions installed in your machine and switch between them easily. You will also avoid issues caused by updating system wide Python.

Follow the instructions in the [pyenv installation guide](https://github.com/pyenv/pyenv#installation) or follow the steps below.

=== "MacOS"

    Install pyenv using Homebrew:
    ```bash
    brew update
    brew install pyenv
    ```

=== "Ubuntu 22.04"

    !!! note "For a more complete installation guide, [follow this guide](https://realpython.com/intro-to-pyenv/#installing-pyenv)."

    Use the automatic installer:

    ```bash
    curl https://pyenv.run | bash
    ```

    For more details visit our other project: https://github.com/pyenv/pyenv-installer


Add these lines to `~/.zshrc`, `~/.bash_profile` or `~/.bashrc`:

```
export PYENV_ROOT="$HOME/.pyenv"
export PATH="$PYENV_ROOT/bin:$PATH"
if command -v pyenv 1>/dev/null 2>&1; then
  eval "$(pyenv init --path)"
  eval "$(pyenv init -)"
fi
```

Restart your shell to apply changes

```
exec "$SHELL"
```

Verify that pyenv is installed properly:

```
pyenv --version
```

Now, you can use pyenv to install and manage multiple Python versions on your Mac. For example, to install Python 3.11.3, run:

```
pyenv install 3.11.3
```

To set the newly installed Python version as the global default, run:

```
pyenv global 3.11.3
```

Now check that `which python3` prints path `.../.pyenv/shims/python3` and `python --version` prints `Python 3.11.3`.


## Clone the project
First of all, you need to have the [ETL project](https://github.com/owid/etl) in your working environment. Run:

```bash
git clone https://github.com/owid/etl.git
```

Along with various directories and files, the project also has sub-packages in the `lib/` folder: `catalog`, `repack` and `datautils`. These redistributable in-house libraries simplify access to data.

## Check your environment

You can get started by using `make` to see available commands. Note that to run all `make` commands you should be in the project folder (as it contains the `Makefile`).

```bash
make help
```

The best way to check if your environment is healthy is to run:

```bash
make test
```

This will install the project, and then run all CI checks.

If `make test` succeeds, then you should be able to build any dataset you like, including the entire catalog. If it fails, please raise a [Github issue](https://github.com/owid/etl/issues) (if OWID staff, you can also ask using the `#tech-issues` Slack channel).


!!! tip

    Speed it up with multiple processes `make -j 4 test`.


## VSCode setup

### Recommended extensions
We highly recommended installing the following extensions:

* [Ruff](https://marketplace.visualstudio.com/items?itemName=charliermarsh.ruff)
* [YAML](https://marketplace.visualstudio.com/items?itemName=redhat.vscode-yaml)

### Custom extensions

#### Find Latest ETL Step
Navigating ETL steps using ++cmd+p++ can be cumbersome, since there are multiple versions and files for a given step.
To simplify this, we created a custom extension that lets you find the latest version of a given ETL step.

To install it, run ++cmd+shift+p++ to open the Command Palette, and select `Extensions: Install from VSIX...`.
Select the latest version of the extension, e.g. `vscode_extensions/find-latest-etl-step/install/find-latest-etl-step-0.0.2.vsix`.
You may need to restart extensions or VSCode afterwards.

To use it, execute ++ctrl+shift+l++; a new text bar will appear at the top; type the name of the ETL step you want to open, and you'll see only the files corresponding to the latest version of that step.

### Additional configuration

Add this to your User `settings.json` (View -> Command Palette -> Preferences: Open User Settings (JSON)):

```json
  "files.associations": {
    "*.dvc": "yaml"
  },
  "yaml.schemas": {
    "schemas/snapshot-schema.json": "**/*.dvc",
    "schemas/dataset-schema.json": ["**/*.meta.yml", "**/*.meta.override.yml"]
  },
```

## Improve your terminal experience

We recommend using [Oh My Zsh](https://ohmyz.sh/). It comes with a lot of plugins and themes that can make your life easier.


???  "Automatic virtualenv activation"

    We use python virtual environments ("venv") everywhere. It's very convenient to have a script that automatically activates the virtualenv when you enter a project folder. Add the following to your `~/.zshrc` or `~/.bashrc`:

    ```bash
    # enters the virtualenv when I enter the folder, provide it's called either .venv or env
    autoload -U add-zsh-hook
    load-py-venv() {
        if [ -f .venv/bin/activate ]; then
            # enter a virtual environment that's here
            source .venv/bin/activate
        elif [ -f env/bin/activate ]; then
            source env/bin/activate
        elif [ ! -z "$VIRTUAL_ENV" ] && [ -f poetry.toml -o -f requirements.txt ]; then
            # exit a virtual environment when you enter a new project folder
            deactivate
        fi
    }
    add-zsh-hook chpwd load-py-venv
    load-py-venv
    ```

    Some staff members also use [Nushell](https://www.nushell.sh/), which supports similar hooks. Edit your `$nu.config-path` file, find the `hooks` section, and add to it an `env_change` stanza:

    ```
    hooks:
      env_change: {
        PWD: [
          {
            condition: {|before, after| ["pyproject.toml" "requirements.txt" "setup.py"] | any {|f| $f | path exists } }
            code: "
                if ('.venv/bin/python' | path exists) {
                print -e 'Activating virtualenv'
                $env.PATH = ($env.PATH | split row (char esep) | filter {|p| $p !~ '.venv' } | prepend $\"($env.PWD)/.venv/bin\")
                } else {
                $env.PATH = ($env.PATH | split row (char esep) | filter {|p| $p !~ '.venv' })
                }
              "
          }
        ]
      }
    ```

??? "Speed up navigation in terminal with autojump"

    Instead of `cd ...` to a correct folder, you can add the following to your `~/.zshrc` or `~/.bashrc`:

    ```bash
    # autojump
    [[ -s `brew --prefix`/etc/autojump.sh ]] && . `brew --prefix`/etc/autojump.sh
    ```

    and then type `j etl` or `j grapher` to jump to the right folder.


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
| `apps/`    | Apps built around and for ETL management. Some include `wizard`, `backport`, `fasttrack`, etc. |
| `data/`    | When you run the recipe code for a dataset, the dataset will be created under this directory. Note that not all the content from this directory is added to git. |
| `schemas/`    | Metadata schemas for ETL datasets. |
| `scripts/`    | Various scripts. |
| `tests/`    | ETL library tests. |
| `lib/`    | Other OWID sub-packages. |
| `docs/`, `.readthedocs.yaml`, `mkdocs.yml`    | Project documentation config files and directory. |
| `Makefile`, `default.mk`    | `make`-related files. |
