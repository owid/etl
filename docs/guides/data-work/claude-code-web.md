---
tags:
  - 👷 Staff
  - Data Workflow
  - AI
icon: lucide/cloud
---

# Claude Code on the web

Create (simple) datasets from your browser — or your phone — with no local
setup at all: no VSCode, no terminal, no sandbox. [Claude Code on the
web](https://claude.ai/code) runs sessions in a cloud environment with the ETL
repository checked out, creates a pull request with a staging server, and
fills in the metadata for you.

This is the low-friction alternative to [Fast-track](https://etl.owid.io/wizard/fasttrack)
(no spreadsheets, full traceability in ETL) and to the
[terminal-based AI workflow](ai-workflow.md) (which remains the right tool for
power users).

## Set up the environment (once)

1. Open <https://claude.ai/code>.
    - **First-time users** are redirected to an onboarding flow: use the name
      `etl` and **`Full`** network access. `Trusted` blocks our own hosts —
      `datasette-public.owid.io`, `api.ourworldindata.org` and
      `catalog.ourworldindata.org` are all refused — which quietly breaks
      indicator lookups and `etl diff`, and the failure looks like an
      authentication error rather than a network one.
    - **Existing users** won't see onboarding: click the environment selector
      (":cloud: Default") above the chat input and create a new `etl`
      environment with the same settings.
2. Edit the `etl` environment and paste the environment variables from
   [1Password](https://ourworldindata.1password.com/app#/E46VV72PBZFZXCCJCLRXIFV4WY/Vault/E46VV72PBZFZXCCJCLRXIFV4WY:7ysaett3c574wa3qsud2olpbde:w354idcwbqxggt2snaz7d5yigi?itemListId=E46VV72PBZFZXCCJCLRXIFV4WY%3A7ysaett3c574wa3qsud2olpbde)
   into **Environment variables** (three lines, `R2_ENDPOINT=...`) → Save
   changes.

    !!! warning

        Environment variables are visible to anyone who can edit the
        environment — only put values there that are okay to share within the
        org (like the 1Password ones above).

3. In the same settings, paste the [setup script](#setup-script) below.
4. Next to the environment selector is the repository picker — choose
   `owid/etl`.

### Setup script

The sandbox image ships an old `uv` and an empty package cache, which makes
every session slow to start and can silently rewrite `uv.lock` (see below).
This script fixes both. It runs once, and the resulting filesystem is
snapshotted, so later sessions skip it entirely; the snapshot rebuilds when you
edit the environment settings and automatically after ~7 days.

```bash
#!/bin/bash
set -euo pipefail

# The sandbox has shipped uv 0.8.17, which cannot parse the relative
# `exclude-newer = "3 days"` in pyproject.toml: it discards the whole [tool.uv]
# table, concludes the lockfile's cutoff was removed, and re-resolves every
# dependency — a ~2000-line uv.lock diff that silently bumps hundreds of
# packages. uv 0.10 is the first release that handles it. The install script is
# used rather than `uv self update`, which hits GitHub API rate limits here.
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"

# The setup script runs before the session's repo is cloned, so clone it here
# to warm the uv cache. The .venv itself cannot be reused (it hard-codes
# absolute paths), but ~/.cache/uv goes into the snapshot, which turns the
# session's own `uv sync` from a multi-minute download into a few seconds.
git clone --depth 1 https://github.com/owid/etl /opt/etl-setup
cd /opt/etl-setup
UV_HTTP_TIMEOUT=300 uv sync --all-extras --group dev
```

Also add `UV_HTTP_TIMEOUT=300` to the environment variables: uv's 30-second
default times out on large wheels (grpcio, torch) through the sandbox's egress
proxy.

Sessions still build their own `.venv` — that part runs from
`scripts/remote_setup.sh`, wired as a `SessionStart` hook — but from a warm
cache and with a current uv.

## Create a dataset

Drag a CSV into the chat (or give Claude a URL with data) and ask:

```
Create a dataset from the attached CSV
```

Claude will create a pull request with a staging server and fill in all the
metadata. It might ask a few clarifying questions, and you can steer it
however you like — ask it to edit metadata, visualise the data in the chat,
add custom processing, and so on.

From there:

1. Create or edit charts on the staging server via its **Admin** (link in the
   PR).
2. When you're happy, approve your changes in **chart-diff** (also linked in
   the PR).
3. Merge the PR — this syncs your charts to production.

## Drive cloud sessions from the terminal

If you already work in the terminal, you don't have to switch to the browser to
use the cloud environment — and you don't have to copy results back by hand.
From the ETL repository:

```bash
claude --cloud "Run the cherry blossom step and report what breaks"
```

This starts a cloud session on the same environment while you keep working
locally. The VM clones from GitHub rather than from your machine, so **push
your branch first**. Then:

- `/tasks` — check on running sessions (each `claude --cloud` is its own
  session, so you can start several in parallel).
- `/teleport` (or `/tp`) — pull a session into your terminal: it checks out the
  branch and loads the full conversation history, so the local session
  continues where the cloud one left off.

Neither flag appears in `claude --help`, but both work.

This is the practical way to debug the cloud environment itself: run
`check-tools` in a session to get the exact versions of everything installed
(the command only exists there), then reproduce locally against that specific
version rather than trying to recreate the whole VM — there is no published
image for it.

!!! note

    Cloud sessions get the repository's `CLAUDE.md`, `.claude/` skills and
    agents, and the `SessionStart` hook, but **not** your personal
    `~/.claude/CLAUDE.md`. Anything a cloud session needs to know has to live
    in the repo — see [`.claude/docs/cloud-sandbox.md`](https://github.com/owid/etl/blob/master/.claude/docs/cloud-sandbox.md).

## Feedback

This workflow is actively evolving. If you try it, share your session or
reach out in #data-scientists on Slack — every attempt improves the dataset
creation skill.
