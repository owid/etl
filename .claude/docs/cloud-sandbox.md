# Cloud sandbox sessions (claude.ai/code)

For agent sessions running in a Claude Code cloud sandbox
(`CLAUDE_CODE_REMOTE=true`). Environment setup is documented in
[docs/guides/data-work/claude-code-web.md](../../docs/guides/data-work/claude-code-web.md).

- **A `uv.lock` diff you didn't intend is a re-resolve, never a real change.**
  Discard it with `git checkout -- uv.lock`.

- **Rename the pre-created `claude/<slug>-<suffix>` branch before pushing** —
  `git branch -m <short-name>`, or let `etl pr` name it (see the root CLAUDE.md).

- **No MySQL here, and staging is on Tailscale**, so `--grapher`, `make query`
  and `OWID_ENV.read_sql` don't work. **This overrides the root CLAUDE.md's
  "always add `--grapher`" rule** — run steps without it and meadow, garden and
  the grapher feather still build. Upserts, chart-diff and the admin happen on
  the PR's staging server after you push.

- **`PREFER_DOWNLOAD=1 .venv/bin/etlr <step> --private`** fetches published
  upstream datasets instead of rebuilding the chain inside the VM. Not usable for
  the step you're building — it isn't in the catalog yet.

- **Verify the build against the catalog rather than by exit code:**
  `.venv/bin/etl diff REMOTE data/ --include <dataset> --verbose`
  (`--output-html` writes a report). Needs no MySQL.

- **OWID hosts require `Full` network access.** Under `Trusted` the gateway
  refuses snapshots, the catalog and Datasette together — a setting to fix, not
  to work around, so ask the user to switch it. `admin.owid.io` never works:
  Cloudflare Access rejects non-browser clients. Creating snapshots with `etls`
  needs the `R2_*` variables (in 1Password — never scrape credentials elsewhere).

- **A refused host looks like an auth failure.** WebFetch reports `403 Forbidden`
  and suggests an authenticated tool; `curl` returns `000`. Confirm the cause
  before hunting for credentials that wouldn't have helped:

  ```bash
  curl -sS "$HTTPS_PROXY/__agentproxy/status" | grep -A4 recentRelayFailures
  ```

  `"kind": "connect_rejected"` means the gateway refused it before TLS.

- **Resolve `admin.owid.io` links through the public Datasette instead of opening
  them:**

  ```bash
  # /admin/variables/<id> — indicator metadata, including unpublished
  curl -s "https://datasette-public.owid.io/owid/variables.json?id__in=<id>&_shape=array"

  # /admin/charts/<id>/edit — config arrives as a JSON string, so unwrap it
  curl -s "https://datasette-public.owid.io/owid/charts.json?id__exact=<id>&_shape=array&_col=config" \
    | .venv/bin/python -c "import json,sys; print(json.dumps(json.loads(json.load(sys.stdin)[0]['config']), indent=2))"
  ```

  `chart_dimensions?chartId__exact=<id>` maps a chart to its indicators; if you
  have the slug, `https://ourworldindata.org/grapher/<slug>.config.json` does it
  in one call. Prefer `?<col>__exact=` filters over `?sql=`, which 403s
  intermittently on long queries. Charts and gdocs are published-only, so an
  empty result usually means a draft — ask the user to paste
  `admin.owid.io/admin/api/charts/<id>.config.json` rather than guessing.

- **After pushing, give the user a deep-linked staging URL** (it takes a while to
  build, especially the first time):

  ```bash
  .venv/bin/python -c "from etl.config import get_container_name; import subprocess; \
    print('http://' + get_container_name(subprocess.check_output(['git','branch','--show-current'], text=True).strip()))"
  ```
