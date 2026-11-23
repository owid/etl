# Cloud sandbox sessions (claude.ai/code)

For agent sessions running in a Claude Code cloud sandbox
(`CLAUDE_CODE_REMOTE=true`). Environment setup is documented in
[docs/guides/data-work/claude-code-web.md](../../docs/guides/data-work/claude-code-web.md).

- **A `uv.lock` diff you didn't intend is a re-resolve, never a real change.**
  Discard it with `git checkout -- uv.lock`.

- **Rename the pre-created `claude/<slug>-<suffix>` branch before pushing** —
  `git branch -m <short-name>`, or let `etl pr` name it (see the root CLAUDE.md).
  If the session may only push to its own branch, keep the name: it's cosmetic,
  and not a reason to delay a PR.

- **No MySQL here, and staging is on Tailscale**, so `--grapher`, `make query`
  and `OWID_ENV.read_sql` don't work. **This overrides the root CLAUDE.md's
  "always add `--grapher`" rule** — run steps without it and meadow, garden and
  the grapher feather still build. Upserts, chart-diff and the admin happen on
  the PR's staging server after you push.

- **A branch gets a staging server only once it has an open PR.** A chart,
  MDim or metadata edit can only be seen there, so open a draft PR
  (`gh pr create --draft`) with the first push, without being asked. Don't
  build `viz://` steps here; the staging build does it.
  Tell the user the moment the preview is ready, in one short message whose
  first line is the full preview link, which works before the step is published:

  ```bash
  # MDim: pass its catalog path. Chart: pass "chart" and its slug.
  .venv/bin/python -c "import sys, subprocess, urllib.parse as u; from etl.config import get_container_name as g; \
    h = 'http://' + g(subprocess.check_output(['git','branch','--show-current'], text=True).strip()); \
    print(h + ('/grapher/' + sys.argv[2] if sys.argv[1] == 'chart' else '/admin/grapher/' + u.quote(sys.argv[1], safe='')))" \
    '<namespace>/latest/<short_name>#<short_name>'
  ```

  Send the link only once it's ready, not with a guess at how long it will
  take. Run this in the background (`run_in_background`), which wakes you when
  it ends:

  ```bash
  sha=$(git rev-parse HEAD)
  for i in $(seq 60); do
    s=$(curl -s "https://api.github.com/repos/owid/etl/commits/$sha/statuses" | .venv/bin/python -c "import json, sys; d = {}; [d.setdefault(x['context'], x['state']) for x in json.load(sys.stdin)]; c = 'buildkite/etl-automated-staging-environment'; print(d.get(c + '/etl-build') or d.get(c) or 'none')")
    case $s in success|failure|error) echo "$s"; break;; esac
    sleep 20
  done
  ```

  It reads the public API without auth, so it works when this session's `gh`
  is limited, and it stays inside the unauthenticated rate limit. The
  `etl-build` status passes once the build has written the push's charts and
  MDims to staging, about a minute after the push; the whole build's status,
  which it falls back to, comes later.

- **`PREFER_DOWNLOAD=1 .venv/bin/etlr <step>`** fetches published
  upstream datasets instead of rebuilding the chain inside the VM. Not usable for
  the step you're building — it isn't in the catalog yet.

- **Verify the build against the catalog rather than by exit code:**
  `.venv/bin/etl diff REMOTE data/ --include <dataset> --verbose`
  (`--output-html` writes a report). Needs no MySQL.

- **OWID hosts require `Full` network access.** Under `Trusted` the gateway
  refuses snapshots, the catalog and `public.duckdb` together — a setting to fix, not
  to work around, so ask the user to switch it. Creating snapshots with `etls`
  needs the `R2_*` variables (in 1Password — never scrape credentials elsewhere).

- **On `admin.owid.io`, only the *authenticated* routes fail.** Cloudflare Access
  rejects non-browser clients, `302`-ing `/admin/*` and authenticated `/api/*` to
  a login page — an app-layer redirect, so `recentRelayFailures` stays empty and
  there are no credentials worth hunting for. It follows that the user can open
  one of these in their browser when you cannot: ask them to paste rather than
  concluding the endpoint is broken. Routes the app serves unauthenticated do
  work: measured from a sandbox, `GET /api/narrative-chart-map` returns its full name→uuid map, while
  `/admin/api/charts/<id>.config.json`, `/api/figma/image` and `POST /api/images`
  all redirect. Test the specific route before concluding the host is closed.

- **A refused host looks like an auth failure.** WebFetch reports `403 Forbidden`
  and suggests an authenticated tool; `curl` returns `000`. Confirm the cause
  before hunting for credentials that wouldn't have helped:

  ```bash
  curl -sS "$HTTPS_PROXY/__agentproxy/status" | grep -A4 recentRelayFailures
  ```

  `"kind": "connect_rejected"` means the gateway refused it before TLS.

- **Resolve `admin.owid.io` links through the public indicator API and
  `public.duckdb` instead of opening them** (`public.duckdb` is the nightly
  public copy of the grapher DB; `/query-grapher-db` has the recipe):

  ```bash
  # /admin/variables/<id> — indicator metadata, also for indicators in no chart
  curl -s "https://api.ourworldindata.org/v1/indicators/<id>.metadata.json"

  # /admin/charts/<id>/edit — config arrives as a JSON string, so unwrap it
  uvx --from duckdb-cli duckdb -noheader -list -c \
    "ATTACH 'https://files.ourworldindata.org/public.duckdb' AS owid (READ_ONLY);
     SET enable_external_access=false; SET lock_configuration=true; USE owid;
     SELECT config FROM charts WHERE id = <id>" \
    | .venv/bin/python -m json.tool
  ```

  `SELECT variableId, property FROM chart_dimensions WHERE chartId = <id>` maps
  a chart to its indicators; if you have the slug,
  `https://ourworldindata.org/grapher/<slug>.config.json` does it in one call.
  On first use DuckDB fetches its `httpfs` extension from
  `extensions.duckdb.org`; where only `files.ourworldindata.org` is reachable,
  download `public.duckdb.gz` from there (~80 MB, ~1 GB unzipped) and `ATTACH`
  the local file, with the same two `SET`s after it. Charts and gdocs are published-only, so an empty result
  usually means a draft — ask the user to paste
  `admin.owid.io/admin/api/charts/<id>.config.json` rather than guessing.

- **MCP connectors are not the reason a session feels slow.** Figma, Slack, Notion
  and GitHub traffic goes to `mcp-proxy.anthropic.com`, which is in `NO_PROXY` —
  it never touches the egress proxy, and measured against a local session the
  sandbox is about **twice as fast per screenshot** (Figma `get_screenshot`, 60
  calls here: median 8.8 s against 12.5–20.5 s locally — the only tool timed in
  both, so read it as the shape of the cost, not a per-tool constant). Every local
  run was taken with Figma's **desktop app open**, and that is *not* the
  explanation: the desktop app serves its own MCP server on a separate path, so a
  session holding `use_figma` is on the hosted connector and never routes through
  the app at all. Quitting it would not move the hosted numbers, and the skill's
  GOTCHAS has the tool-by-tool enumeration that settles it. What a
  sandbox pays instead is **nothing per turn that a local session doesn't** — the
  turn tracks the work in it, not the environment, and identical light probes
  measured **2.8 s in a sandbox against 3.7 s locally**. A ~12 s median was once
  billed to the cloud here; it came from 23 turns doing real chart work, so read
  it as the *heavy-turn* cost a real run pays either side. Which leaves the
  sandbox faster on every term measured so far, and a skill
  making hundreds of MCP calls won or lost on batching: issue independent
  calls in one message, 4–6 at a time. Ten reps of a fixed six-call probe on each
  side put that at **≈4.0× in both** (cloud 4.00×, local 3.84×), so batching is
  environment-neutral even though the calls are not: cloud median 8.75 s per
  screenshot and 13.1 s per batch, local 13.91 s and 22.5 s. **Those are `sum/wall`,
  which counts a queued call's own wait as work** — against a serial arm the honest
  local gain is 2.8–3.2×, and the figures are comparable to each other rather than
  to a stopwatch. And all of it is `get_screenshot`: batching `use_figma` does not
  compress server time at all (0.8–1.1× on the calls, since a Figma file runs plugin
  scripts one at a time) and pays only by collecting the turn gaps — still worth
  doing, and worth *more* in a sandbox, where a 0.70 s call leaves the turn ~80% of
  the cost against ~46% locally. A batch's wall is
  `first call + rate × (n−1)` — 9.2 s + 0.75 s here, 11.7 s + 2.1 s locally —
  so the sandbox is near-parallel while a local session pipelines. Net effect: the
  sandbox is the faster side either way, and batching *widens* the gap rather than
  closing it — at one turn per call it is 11.6 s against 17.6 s (1.5×), and at one
  turn per six-call batch 2.7 s against 4.4 s per call (1.6×). One smaller tax
  *is* sandbox-specific: HTTP GETs to OWID and CDN hosts pay
  0.6–2.3 s of proxy overhead each where a local
  session sees 0.05–0.3 s (parallelize batches with `xargs -P`). One is not —
  whether MCP tool schemas arrive deferred is a harness setting, not an
  environment, and a local session gets them that way too, so read your own
  session's tool list rather than inferring it from the environment; where they
  are deferred, load the ones a run needs in a single `ToolSearch` instead of
  one per turn.
