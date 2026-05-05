---
tags:
  - 👷 Staff
  - Development
icon: lucide/git-pull-request
---

We use pull requests (PRs) to propose changes to the codebase. They are the best way to suggest changes to the codebase, and they are also the best way to get feedback on your work.

Every PR in the ETL repository has an associated [staging server](staging-servers) created to it. To smooth this process, we have automated all of this with the command `etl pr`.

!!! tip "Learn more about how to use the [`etl pr` command](etl-cli/#etl-pr){data-preview}"

## PR work summary
Once you've created a PR, the automation user [@owidbot](https://github.com/owidbot) will add a comment to the PR summarizing your work in the PR and providing links to relevant resources. This comment will include the following information:

- Quick links: Links to the site and tooling using changes introduced by the PR. This includes admin site, public site, Wizard, documentation.
- Login: Instructions on how to ssh into the staging server.
- chart-diff: Wizard app showing chart changes (if any) compared to PRODUCTION. Owidbot will complain if there are chart changes pending review.
- data-diff: Changes introduced in the data compared to PRODUCTION.

<figure markdown="span">
    <img src="../../assets/pr-1.png" alt="Chart Upgrader" style="width:80%;">
    <figcaption>PR, and [comment by @owidbot](https://github.com/owid/etl/pull/3563#issuecomment-2485397175), as of 19th November 2024</figcaption>
</figure>


## Scheduling a PR merge

You can schedule a PR merge by using the command `/schedule` at the end of your PR description. This is useful whenever you want to merge your PR at a specific time, e.g. nightly if it could trigger a long deployment process in the main branch.

You have multiple options to schedule a PR merge:

- `/schedule`: The PR will be merged at the next sharp hour (e.g., 13:00, 14:00), based on the current UTC time.
- `/schedule 2024-11-19`: The PR will be merged at midnight (00:00 UTC) on the specified date.
- `/schedule 2024-11-19T12:50:00.000Z`: The PR will be merged at the next sharp hour immediately following the specified timestamp (e.g., if scheduled for 12:50 UTC, it will merge at 13:00 UTC).

You can find an example [:fontawesome-brands-github: here](https://github.com/owid/etl/pull/3563).

<figure markdown="span">
    <img src="../../assets/pr-2.png" alt="Chart Upgrader" style="width:100%;">
    <figcaption>[GitHub action comment](https://github.com/owid/etl/pull/3563#issuecomment-2485414940), as of 19th November 2024</figcaption>
</figure>

## Working on multiple branches in parallel

If you need several PRs in flight at once (e.g. several agent sessions, one per branch), `etl pr --worktree` creates the new branch in a separate **git worktree** so your current working tree is untouched:

```bash
etl pr "Update some dataset" data --worktree
```

The command creates the worktree at `../etl-<branch>` and runs `uv sync` inside it, so the worktree's `.venv/` is ready to use by the time the command finishes. To start working there:

```bash
cd ../etl-<branch>
```

Otherwise also run `source .venv/bin/activate`. Or, even better, set up auto-activation once (see below) — then `cd` alone is enough.

When you're done with the worktree (typically after the PR is merged), clean up:

```bash
git worktree remove ../etl-<branch>
git branch -D <branch>
```

### Optional: auto-activate the venv when you `cd`

Add this snippet to your `~/.zshrc` so the right `.venv/` activates automatically every time you `cd` into a worktree (or any project folder with a `.venv/`):

```zsh
autoload -U add-zsh-hook
load-py-venv() {
    if [ -f .venv/bin/activate ]; then
        source .venv/bin/activate
    elif [ -f env/bin/activate ]; then
        source env/bin/activate
    elif [ -f venv/bin/activate ]; then
        source venv/bin/activate
    elif [ ! -z "$VIRTUAL_ENV" ] && [ -f poetry.toml -o -f requirements.txt ]; then
        deactivate
    fi
}
add-zsh-hook chpwd load-py-venv
load-py-venv
```

After reloading your shell (`source ~/.zshrc` or open a new terminal), you can `cd` between worktrees and the matching venv will activate on its own.

Tip: open each worktree in its own VS Code window (`File > New Window`). The Claude Code extension is scoped per workspace, so each window gets its own chat.

### Sharing the data folder (optional)

`--share-data` symlinks the new worktree's `data/` to the original's, so upstream ETL steps don't get recomputed:

```bash
etl pr "Update dataset" data --worktree --share-data
```

!!! warning
    Never run `rm -rf data/` in a shared worktree — the trailing slash makes `rm` follow the symlink and wipe the **original** `data/`. Use `git worktree remove ../etl-<branch>` to clean up instead.
