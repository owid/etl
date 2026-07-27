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

The sandbox ships a `uv` too old to read our `pyproject.toml` (it silently
rewrites `uv.lock`) and an empty package cache, so every session starts slowly.
This script fixes both. It runs once and the filesystem is snapshotted, so later
sessions skip it; the snapshot rebuilds when you edit the environment settings
and automatically after ~7 days.

```bash
#!/bin/bash
set -euo pipefail

# uv >= 0.10 is required. `uv self update` hits GitHub API rate limits here.
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"

# This runs before the session's repo is cloned, so clone it here to warm
# ~/.cache/uv, which the snapshot keeps. (The .venv itself can't be reused — it
# hard-codes absolute paths — but sessions rebuild it in seconds from cache.)
git clone --depth 1 https://github.com/owid/etl /opt/etl-setup
cd /opt/etl-setup
UV_HTTP_TIMEOUT=300 uv sync --all-extras --group dev
```

Also add `UV_HTTP_TIMEOUT=300` to the environment variables. `--all-extras`
pulls 4.1 GB of linux-only torch/CUDA wheels; downloading them saturates the
sandbox's egress proxy until some other download stalls past uv's 30-second read
timeout.

Sessions still build their own `.venv` via `scripts/remote_setup.sh`, wired as a
`SessionStart` hook.

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
locally. The VM clones from GitHub rather than from your machine, so **push your
branch first**. Then `/tasks` lists running sessions (each `claude --cloud` is
its own, so they can run in parallel), and `/teleport` (or `/tp`) pulls one into
your terminal with the branch checked out and the full conversation history
loaded. Neither flag appears in `claude --help`, but both work.

To debug the environment itself, run `check-tools` in a session for exact tool
versions — the command only exists there — then reproduce against that version
locally. There is no published image to run.

!!! note

    Cloud sessions get the repository's `CLAUDE.md`, `.claude/` skills and
    agents, and the `SessionStart` hook, but **not** your personal
    `~/.claude/CLAUDE.md`. Anything a cloud session needs to know has to live
    in the repo — see [`.claude/docs/cloud-sandbox.md`](https://github.com/owid/etl/blob/master/.claude/docs/cloud-sandbox.md).

## Feedback

This workflow is actively evolving. If you try it, share your session or
reach out in #data-scientists on Slack — every attempt improves the dataset
creation skill.
