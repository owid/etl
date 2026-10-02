---
icon: lucide/hard-drive-download
tags:
  - Setup
---

# Installation
The use of ETL is mainly addressed to OWID staff, but open to the general public. It is supported and regularly run on Linux, MacOS and Windows via WSL. Here's how to get set up.

!!! warning

    Some parts of ETL rely on other internal tools and resources, making it less suitable for external uses. Still, we believe that there is value in having this project open to the public for transparency and reproducibility purposes.


*[WSL]: Windows Subsystem for Linux

## Install dependencies

You will need Python 3.11+, basic build tools, and MySQL client libraries.

=== "MacOS"

    !!! tip

        We recommend using [:octicons-link-external-16: Homebrew](https://brew.sh/) to install dependencies.

    Ensure you have XCode command line tools:

    ```bash
    xcode-select --install
    ```

    Then install Python 3.11+ and MySQL client and [:octicons-link-external-16: UV](https://docs.astral.sh/uv/). UV is our preferred python packaging and dependency management tool.

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

---

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

Even though it's not compulsory, it is **highly recommended** to install [:fontawesome-brands-github: pyenv](https://github.com/pyenv/pyenv#installation) to manage your Python versions. This will allow you to have multiple Python versions installed in your machine and switch between them easily. You will also avoid issues caused by updating system wide Python.

Follow the instructions in the [:fontawesome-brands-github: pyenv installation guide](https://github.com/pyenv/pyenv#installation) or follow the steps below.

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
---


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

Now, you can use pyenv to install and manage multiple Python versions on your Mac. For example, to install Python 3.12.0, run:

```
pyenv install 3.12.0
```

To set the newly installed Python version as the global default, run:

```
pyenv global 3.12.0
```

Now check that `which python3` prints path `.../.pyenv/shims/python3` and `python --version` prints `Python 3.12.0`.


## Clone the project
First of all, you need to have the [:fontawesome-brands-github: ETL project](https://github.com/owid/etl) in your working environment. Run:

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

If `make test` succeeds, then you should be able to build any dataset you like, including the entire catalog. If it fails, please raise a [:fontawesome-brands-github: Github issue](https://github.com/owid/etl/issues) (if OWID staff, you can also ask using the `#tech-issues` Slack channel).


!!! tip

    Speed it up with multiple processes `make -j 4 test`.

## Git hooks

The pre-commit hook is activated automatically by `make .venv` (and any target that depends on it). It runs `make check` (lint, format, type-check) before every `git commit`, which prevents accidentally pushing code that fails CI. Because `make check` *fixes* what it can, the hook re-stages the files it rewrote, so the commit carries the corrected code — you don't need to run `make check` yourself first. It refuses to touch a file that is only partially staged, since re-staging one would pull its unstaged changes into the commit.

If you need to (re)activate it manually:

```bash
make install-hooks
```

It is installed as a symlink at `$GIT_DIR/hooks/pre-commit`, git's default location. Note that it deliberately does *not* set `core.hooksPath`: that replaces the hooks directory rather than adding to it, and a repo-local setting beats a global one, so it would silently disable any machine-wide hooks you have.

## Working in a git worktree

`.env` is gitignored, so `git worktree add` cannot carry it into a new worktree, and a worktree starts with no `data/` either — so every ETL step would recompute from snapshots. `make setup.worktree` handles both, runs automatically as part of `make .venv`, and never overwrites anything already there.

```bash
git worktree add ../etl-mybranch -b mybranch
cd ../etl-mybranch
make setup.worktree   # .env and a copy-on-write clone of data/ — offline, a few seconds
make .venv            # only when you actually need the venv (~17 s)
```

`make .venv` runs `setup.worktree` itself, so a single `make .venv` does everything — and so does any other make target, since they all depend on `.venv`. You only need `setup.worktree` on its own when you want the config without paying for a venv.

Copying 16 GB of `data/` sounds absurd and isn't: on a copy-on-write filesystem (APFS on macOS) `cp -c` clones the files, sharing the original's blocks until something writes to them. Measured on a 16 GB / ~32,000-file `data/`: **4 seconds and ~0 bytes**. So a real copy is *better* than symlinking it — each worktree gets an independent `data/`, and two of them running the same step cannot overwrite each other's output. `setup.worktree` checks the filesystem before copying and leaves `data/` absent when cloning isn't possible, rather than making a real 16 GB copy — see the trap below, `cp -c` cannot be trusted to refuse.

The venv is deliberately not part of `setup.worktree`. It is the one slow, network-dependent step — about 17 seconds for 412 packages, against a few seconds and ~0 bytes for config and `data/` — and plenty of worktrees never need one. Since every make target depends on `.venv`, it gets built the moment something actually needs it. If you reach for `.venv/bin/etlr` in a worktree that hasn't built one yet you'll get `no such file or directory`; `make .venv` fixes it.

Two things not to try instead. **Symlinking `.venv`** to the main checkout looks tempting and is a trap twice over: the venv holds an *absolute* editable pointer to the main checkout, so `.venv/bin/etlr` would silently run the main checkout's code rather than your branch's — and `uv sync` follows the symlink rather than replacing it, so any make target in that worktree rewrites the *shared* venv, uninstalling packages the main checkout needs. **Cloning `.venv`** copy-on-write is no faster than building it (a venv is ~103,000 small files, so per-file clone cost dominates) and has the same wrong-code problem.

### Copy-on-write: there is nothing to turn on

Worth stating plainly, because it is easy to assume otherwise: **copy-on-write is not a setting you enable.** On macOS, APFS supports file cloning out of the box, on every Mac, with no configuration. What you have to do is *ask for it*, per command — plain `cp` does a full byte-for-byte copy:

```bash
cp -Rc  src dst            # macOS / BSD cp: clone (copy-on-write)
cp -R   src dst            # a real copy — the default

cp -R --reflink=auto src dst   # GNU cp (Linux): clone if possible, else copy
```

Both paths must be on the **same APFS volume**. And here is the trap: `cp -c` does **not** fail when it can't clone — it silently makes a real copy. Verified by copying from APFS to a mounted HFS+ image: the file was created, no error. So in a script, checking the filesystem *first* is the only safe approach; `cp -c` succeeding tells you nothing about whether it cloned:

```bash
# a script that must never make a real copy of something huge
SRC_DEV=$(df -P "$src" | awk 'NR==2{print $1}')
DST_DEV=$(df -P . | awk 'NR==2{print $1}')
[ "$SRC_DEV" = "$DST_DEV" ] || exit 0     # different filesystems: it could only be a copy
```

(GNU `cp` on Linux has no `-c` at all — it errors out — and `--reflink=always` does fail honestly when the filesystem can't clone. Only the macOS flag is silently forgiving.)

To check a clone really happened, compare **free space**, not `du`:

```bash
df -k /System/Volumes/Data | tail -1 | awk '{print $4}'   # before
cp -Rc big-dir clone-dir
df -k /System/Volumes/Data | tail -1 | awk '{print $4}'   # after — expect no change
```

`du` reports *apparent* size and counts a clone (and a hard link) at full size, so it will tell you the copy cost 16 GB when it cost nothing. The same caveat makes `du` useless for sizing a `.venv`: `uv` hardlinks packages out of `~/.cache/uv`, so a 2.3 GB-looking venv adds well under 200 MB of real disk.

**On Linux** it depends on the filesystem: XFS (`reflink=1`, the default since 2018), btrfs and ZFS support it; **ext4 does not** and never has, so `cp --reflink=always` fails there. `setup.worktree` reports that and leaves `data/` absent.

The trick isn't specific to `data/`. It applies to any large directory you'd otherwise duplicate or symlink: `node_modules`, a dataset, or the checkout itself on a very large repo.

### Provisioning worktrees automatically

`setup.worktree` is a deliberately boring, shared name — owid-grapher uses it too — so anything that creates a worktree can provision *any* repo without knowing what that repo needs. Two ways to hook it up:

**A worktree manager.** Point its per-repo setup script at `make setup.worktree`. Nothing else is needed.

**A machine-wide git hook**, if you create worktrees with plain `git` and want this to happen by itself. Set `core.hooksPath` to a directory of your own and put this in its `post-checkout`:

```bash
#!/bin/sh
# Only when the checkout is brand new: git passes the all-zero OID as the previous
# HEAD for `git worktree add` and `git clone`, and the real old HEAD for an
# ordinary branch switch. Without this the hook would fire on every `git checkout`.
case "$1" in '' | *[!0]*) exit 0 ;; esac
[ "$3" = "1" ] || exit 0

# Nothing to provision in the primary checkout — it's the source everything copies from.
[ "$(git rev-parse --git-dir)" = "$(git rev-parse --git-common-dir)" ] && exit 0

cd "$(git rev-parse --show-toplevel)" || exit 0
# `make -n` resolves the target without running it, and exits non-zero if the repo
# has none — so this is a no-op in repos that don't define it.
make -n setup.worktree >/dev/null 2>&1 && make setup.worktree
exit 0
```

With this in place `git worktree add` alone gives a worktree its config and `data/` in a few seconds, and the venv follows the first time you run a make target in it.

Two things to know if you do this. `core.hooksPath` **replaces** `$GIT_DIR/hooks` rather than adding to it, so a dispatcher of this kind should end by exec'ing the repo's own `$GIT_DIR/hooks/<name>` — otherwise it silently disables every per-repo hook, including this repo's pre-commit. And keep `setup.worktree` cheap and offline for the same reason: it runs inside a git hook, which is why it does not build the venv.

## GitHub Actions

We use [:octicons-link-external-16: pinact](https://github.com/suzuki-shunsuke/pinact)
to manage GitHub Actions and workflow versions. Action references in
`.github/workflows/*` and `.github/actions/*` should be pinned to immutable commit SHAs
with a version comment, rather than to a mutable tag like `@v5`. Pinning to a SHA means a
compromised or retagged action can't silently change what runs in our CI, while the
trailing comment keeps the human-readable version visible.

Run `pinact run -update` to update and pin every action and workflow in the repository.

## VSCode setup

### Recommended extensions
We highly recommended installing the following extensions:

* [:octicons-link-external-16: Ruff](https://marketplace.visualstudio.com/items?itemName=charliermarsh.ruff)
* [:octicons-link-external-16: YAML](https://marketplace.visualstudio.com/items?itemName=redhat.vscode-yaml)

### Custom ETL extensions

We've built custom VS Code extensions to streamline ETL development. To install all extensions:

```bash
make vsce-sync
```

This includes extensions for navigating ETL steps, debugging interactively, comparing versions, and detecting outdated code patterns.

For detailed information about each extension and how to use them, see the **[VS Code Extensions Guide](../guides/vscode-extensions.md)**.

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

!!! tip "Using [:octicons-link-external-16: Oh My Zsh](https://ohmyz.sh/)."

    We recommend using Oh My Zsh. It comes with a lot of plugins and themes that can make your life easier.


### Automatic virtualenv activation

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

Some staff members also use [:octicons-link-external-16: Nushell](https://www.nushell.sh/), which supports similar hooks. Edit your `$nu.config-path` file, find the `hooks` section, and add to it an `env_change` stanza:

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

### Speed up navigation in terminal with autojump

Instead of `cd ...` to a correct folder, you can add the following to your `~/.zshrc` or `~/.bashrc`:

```bash
# autojump
[[ -s `brew --prefix`/etc/autojump.sh ]] && . `brew --prefix`/etc/autojump.sh
```

and then type `j etl` or `j grapher` to jump to the right folder.
