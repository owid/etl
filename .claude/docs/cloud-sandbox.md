# Cloud sandbox sessions (claude.ai/code)

Notes for agent sessions running in a Claude Code cloud sandbox
(`CLAUDE_CODE_REMOTE=true`). For how the environment itself is set up (setup
script, environment variables), see
[docs/guides/data-work/claude-code-web.md](../../docs/guides/data-work/claude-code-web.md).

- **Never commit a `uv.lock` diff that appeared on its own.** `uv sync` only
  rewrites the lockfile when it decided to re-resolve, which here means the
  sandbox's uv was too old to parse `exclude-newer = "3 days"` in
  `pyproject.toml` (needs >= 0.10). The diff is ~2000 lines and silently bumps
  hundreds of packages. Discard it with `git checkout -- uv.lock`. The
  `SessionStart` hook installs a current uv and warns if the lockfile went
  dirty, but check `git status` before committing regardless.

- **Rename the pre-created branch before the first push.** The sandbox creates
  `claude/<slug>-<random-suffix>`, which violates the repo's branch-naming
  rules (short, no prefix — see the root CLAUDE.md). Use
  `git branch -m <short-descriptive-name>`, or let `etl pr` create a properly
  named branch.

- **There is no MySQL in the sandbox and staging servers are on Tailscale.** So
  `--grapher` (the MySQL upsert), `make query`, and `OWID_ENV.read_sql` are not
  available. Run steps without `--grapher`: that still builds meadow, garden,
  and the grapher dataset feather, which is enough to verify the data. Anything
  DB-side — variable upserts, chart-diff, the admin — happens on the PR's
  staging server after you push.

- **Snapshot downloads work** (`snapshots.owid.io` is reachable), so `etlr` can
  resolve dependencies normally. Creating a *new* snapshot with `etls` needs the
  `R2_*` environment variables; if they're missing, the environment was set up
  without them and the user needs to add them (they're in 1Password — never
  scrape credentials from anywhere else).

- **After pushing, hand the user a staging link**, deep-linked to the affected
  chart or page. Don't derive the name by hand:

  ```bash
  .venv/bin/python -c "from etl.config import get_container_name; import subprocess; \
    print('http://' + get_container_name(subprocess.check_output(['git','branch','--show-current'], text=True).strip()))"
  ```

  Mention that the staging server takes a while to build after a push,
  especially the first one.
