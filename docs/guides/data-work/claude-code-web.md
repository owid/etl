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
      `etl` and `Trusted` or `Full` network access (you can switch to `Full`
      later if `Trusted` turns out to be limiting).
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

3. Next to the environment selector is the repository picker — choose
   `owid/etl`.

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

## Feedback

This workflow is actively evolving. If you try it, share your session or
reach out in #data-scientists on Slack — every attempt improves the dataset
creation skill.
