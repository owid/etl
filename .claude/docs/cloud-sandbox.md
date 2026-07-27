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
  and the grapher dataset feather. Anything DB-side — variable upserts,
  chart-diff, the admin — happens on the PR's staging server after you push.

- **To check a build without a database, diff it against the remote catalog:**
  `.venv/bin/etl diff REMOTE data/ --include <dataset>` (add `--verbose`, or
  `--output-html` for a report). This answers "did my rebuild change any
  values?", which is most of why you'd run a step by hand, and needs no MySQL —
  but it does fetch from `catalog.ourworldindata.org`, so confirm that host is
  reachable first (see below). "The step completed" is not the same as "the data
  is right"; say which one you verified.

- **Don't assume any OWID host is reachable — check the one you need.** Egress
  goes through a gateway proxy that allows hosts by policy, and the policy is
  not the same in every environment. `snapshots.owid.io` has worked (so `etlr`
  resolves dependencies normally), while `datasette-public.owid.io`,
  `api.ourworldindata.org` and `catalog.ourworldindata.org` have all been denied
  in at least one session. One `curl -s -o /dev/null -w "%{http_code}\n" <url>`
  settles it in a second — do that before building a plan on top of a host.
  `admin.owid.io` is the exception that never works: Cloudflare Access rejects
  every non-browser client at the edge, whatever the proxy allows.

  Creating a *new* snapshot with `etls` needs the `R2_*` environment variables;
  if they're missing, the environment was set up without them and the user needs
  to add them (they're in 1Password — never scrape credentials from anywhere
  else).

- **A blocked host looks like an auth problem — it isn't.** WebFetch reports a
  gateway denial as `HTTP 403 Forbidden` with the hint *"If this URL requires
  authentication, use an authenticated tool"*, and `curl` just returns `000`.
  Chasing that hint means hunting for credentials that would not have helped.
  Confirm the real cause before concluding anything:

  ```bash
  curl -sS "$HTTPS_PROXY/__agentproxy/status" | grep -A4 recentRelayFailures
  ```

  `"kind": "connect_rejected"` names the host the gateway refused — the request
  died before TLS, so no key, token, or login would have changed the outcome.
  Report it as a sandbox network limit and ask the user to allowlist the host,
  rather than working around it.

- **Admin links can't be opened; resolve the id instead.** When the user shares
  an `admin.owid.io` link, don't fetch it — pull the id out of the URL and query
  the public Datasette mirror, which needs no auth.

  `/admin/variables/<id>` — indicator metadata, including unpublished
  indicators:

  ```bash
  curl -s "https://datasette-public.owid.io/owid/variables.json?id__in=<id>&_shape=array"
  ```

  `/admin/charts/<id>/edit` — the chart's full config. It comes back as a JSON
  string inside the row, so unwrap it:

  ```bash
  curl -s "https://datasette-public.owid.io/owid/charts.json?id__exact=<id>&_shape=array&_col=config" \
    | .venv/bin/python -c "import json,sys; print(json.dumps(json.loads(json.load(sys.stdin)[0]['config']), indent=2))"
  ```

  `chart_dimensions` maps that chart to its indicators
  (`?chartId__exact=<id>&_shape=array`), so the two together answer "what is
  this chart built from". If you already have the slug rather than the id,
  `https://ourworldindata.org/grapher/<slug>.config.json` returns the same thing
  in one call.

  Filter with `?<col>__in=` / `?<col>__exact=` and select columns with repeated
  `&_col=`. The `?sql=` form documented in the root CLAUDE.md works too, but a
  long encoded query has 403'd intermittently, so prefer the filter form for
  simple lookups.

  Charts and gdocs in Datasette are **filtered to published only**, so an empty
  result for a plausible chart id usually means it's a draft, not that the id is
  wrong. Drafts are only reachable from an authenticated browser: ask the user
  to open `admin.owid.io/admin/api/charts/<id>.config.json` and paste it. Never
  guess what an id refers to — say you couldn't resolve it and ask.

- **After pushing, hand the user a staging link**, deep-linked to the affected
  chart or page. Don't derive the name by hand:

  ```bash
  .venv/bin/python -c "from etl.config import get_container_name; import subprocess; \
    print('http://' + get_container_name(subprocess.check_output(['git','branch','--show-current'], text=True).strip()))"
  ```

  Mention that the staging server takes a while to build after a push,
  especially the first one.
