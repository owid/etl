"""End-to-end check of what chart-diff lists and what chart-sync writes, for ETL-authored charts.

Unit tests cover the comparison functions. This covers the thing they cannot: the actual
behaviour against a grapher database, which is where every bug in this area has come from.

    .venv/bin/python scripts/chart_sync_scenarios.py --staging my-branch

It plays STAGING against a stand-in for PRODUCTION, and walks through the situations that
decide whether an edit survives:

    S0  a staging build re-pushes an unchanged ETL layer       -> chart must not be listed
    S1  the chart is edited in production only                 -> not listed on other branches
    S2  the branch rebuilds the chart                          -> listed; conflict iff production also changed
    S3  the chart is edited in the staging admin               -> listed
    S4  approved, then production is edited                    -> approval expires, sync refuses
    S5  a chart the branch created, then edited in production  -> sync refuses
    S6  corner case normal work never reaches: see the note under S6 below

Read the PASS/FAIL lines, not the prose: each check states what should happen and prints what
did. Setup checks come first, so a failure there means the situation was never reproduced and
the verdict below it is meaningless.

WHAT IT WRITES. Everything happens on the staging server you name:

  - creates a database `owid_prodsim` next to `owid`, holding a copy of the tables chart-diff and
    chart-sync read; this stands in for production, and is dropped at the end;
  - edits one existing ETL-authored chart (default 7118, chick culling) and restores it, including
    its edit timestamps, so the script can be re-run;
  - creates a throwaway chart and deletes it.

Chart-sync only ever runs with --dry-run, and only against the copy. Production is never touched
and no production credentials are read. Needs Tailscale to reach the staging server.
"""

import copy
import json
import os
import re
import subprocess
import sys
import time

import rich_click as click
from sqlalchemy import text
from sqlalchemy.orm import Session

from apps.chart_sync.admin_api import AdminAPI
from apps.wizard.app_pages.chart_diff.chart_diff import ChartDiffsLoader
from etl.config import Config, OWIDEnv
from etl.paths import BASE_DIR

# The stand-in for production: a second database on the same MySQL server.
PROD_DB = "owid_prodsim"

# The throwaway chart S5 creates. Fixed, so a re-run after a crash reuses it instead of piling up.
NEW_UUID = "0198c0e8-1111-7000-8000-000000000001"
NEW_PATCH_UUID = "0198c0e8-1111-7000-8000-000000000002"
NEW_SLUG = "zz-chart-sync-scenarios-test-chart"
NEW_CATALOG_PATH = "animal_welfare/latest/chart_sync_scenarios#chart_sync_scenarios"

# Tables the copy needs. chart_configs and variables are filtered (they are large).
COPIED_TABLES = [
    "charts", "chart_dimensions", "datasets", "chart_tags", "tags", "chart_slug_redirects",
    "chart_diff_approvals", "chart_diff_conflicts", "users", "chart_revisions", "narrative_charts", "dods",
]


class Scenarios:
    def __init__(self, staging_name: str, chart_id: int, user_id: int):
        self.staging = OWIDEnv.from_staging(staging_name)
        if self.staging.conf.DB_NAME == "live_grapher":
            raise click.ClickException("Refusing to run against production.")
        self.production = OWIDEnv(
            Config(
                GRAPHER_USER_ID=self.staging.conf.GRAPHER_USER_ID,
                DB_USER=self.staging.conf.DB_USER,
                DB_NAME=PROD_DB,
                DB_PASS=self.staging.conf.DB_PASS,
                DB_PORT=self.staging.conf.DB_PORT,
                DB_HOST=self.staging.conf.DB_HOST,
            )
        )
        self.staging_name = staging_name
        self.api = AdminAPI(self.staging)
        self.chart_id = chart_id
        self.user_id = user_id
        self.results: list[tuple[str, bool, str]] = []
        self.new_chart_id: int | None = None
        self.config_uuid: str = ""
        self.catalog_path: str | None = None

    # -------------------------------------------------------------- small helpers
    def sql(self, env: OWIDEnv, statement: str, **params) -> None:
        with env.engine.begin() as con:
            con.execute(text(statement), params)

    def row(self, env: OWIDEnv, chart_id: int | None = None):
        chart_id = chart_id or self.chart_id
        return env.read_sql(
            "SELECT c.updatedAt, c.lastEditedAt, c.lastEditedByUserId, "
            "JSON_UNQUOTE(JSON_EXTRACT(cc.config,'$.subtitle')) AS subtitle, "
            "JSON_UNQUOTE(JSON_EXTRACT(cc.config,'$.note')) AS note "
            f"FROM charts c JOIN chart_configs cc ON cc.id = c.configId WHERE c.id = {chart_id}"
        ).iloc[0]

    def show(self, label: str, env: OWIDEnv, chart_id: int | None = None):
        r = self.row(env, chart_id)
        print(f"  {label}: updatedAt={r.updatedAt} lastEditedAt={r.lastEditedAt} "
              f"subtitle={str(r.subtitle)[:38]!r} note={str(r.note)[:38]!r}")
        return r

    def check(self, name: str, ok: bool, detail: str) -> None:
        self.results.append((name, ok, detail))
        print(f"  {'PASS' if ok else 'FAIL'}  {name}  [{detail}]")

    def diff(self, chart_id: int | None = None):
        """What chart-diff shows for one chart, staging vs the production copy."""
        chart_id = chart_id or self.chart_id
        loader = ChartDiffsLoader(self.staging.get_engine(), self.production.get_engine(), chart_ids=[chart_id])
        diffs = loader.get_diffs(sync=True, chart_ids=[chart_id], skip_analytics=True)
        return {d.chart_id: d for d in diffs}.get(chart_id)

    @staticmethod
    def describe(d) -> str:
        if d is None:
            return "not listed"
        return f"listed (conflict={d.in_conflict}, status={d.approval_status})"

    def approve(self, chart_id: int) -> None:
        """Approve a chart in chart-diff, as a reviewer would."""
        with Session(self.staging.get_engine()) as s, Session(self.production.get_engine()) as t:
            loader = ChartDiffsLoader(self.staging.get_engine(), self.production.get_engine(),
                                      chart_ids=[chart_id])
            diffs = loader.get_diffs(sync=True, chart_ids=[chart_id], skip_analytics=True,
                                     source_session=s, target_session=t)
            if not diffs:
                raise click.ClickException(
                    f"Chart {chart_id} is not listed in chart-diff, so there is nothing to approve. "
                    "The scenario did not reach the state it needs; the checks above say where it stopped."
                )
            diffs[0].approve(s)

    def chart_sync(self, chart_id: int | None = None, ignore_conflicts: bool = False) -> tuple[dict[str, bool], str]:
        """Run chart-sync --dry-run and report which decision it took for this chart."""
        chart_id = chart_id or self.chart_id
        extra = ["--ignore-conflicts"] if ignore_conflicts else []
        env_file = BASE_DIR / f".env.{PROD_DB}"
        c = self.staging.conf
        env_file.write_text(
            f"DB_USER={c.DB_USER}\nDB_NAME={PROD_DB}\nDB_PASS={c.DB_PASS}\nDB_PORT={c.DB_PORT}\n"
            f"DB_HOST={c.DB_HOST}\nGRAPHER_USER_ID={c.GRAPHER_USER_ID or ''}\n"
        )
        try:
            proc = subprocess.run(
                [sys.executable, "-c", "from apps.chart_sync.cli import cli; cli()",
                 f"staging-site-{self.staging_name}", str(env_file), "--dry-run", *extra],
                capture_output=True, text=True, cwd=str(BASE_DIR), env=os.environ.copy(), timeout=900,
            )
        finally:
            env_file.unlink(missing_ok=True)
        # `\b` stops "target_chart_id=<same number>" from matching: the character before it is "_",
        # which is a word character, so there is no boundary there.
        mine = [ln for ln in (proc.stdout + proc.stderr).splitlines() if re.search(rf"\bchart_id={chart_id}\b", ln)]
        took = {
            "update": any("chart_sync.chart_update" in ln for ln in mine),
            "create": any("chart_sync.chart_create" in ln for ln in mine),
            "pending": any("chart_sync.pending_chart" in ln for ln in mine),
            "blocked": any("chart_sync.target_edited_after_creation" in ln for ln in mine),
            "skip": any("chart_sync.skip" in ln for ln in mine),
        }
        return took, " ".join(f"{k}={v}" for k, v in took.items()) + f" exit={proc.returncode}"

    # -------------------------------------------------------------- environment setup
    def build_production_copy(self) -> None:
        print(f"Building the production stand-in ({PROD_DB}) ...")

        def columns(con, table: str) -> str:
            names = con.execute(
                text("SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = :db "
                     "AND TABLE_NAME = :t AND EXTRA NOT IN ('STORED GENERATED', 'VIRTUAL GENERATED') "
                     "ORDER BY ORDINAL_POSITION"),
                {"db": self.staging.conf.DB_NAME, "t": table},
            ).scalars().all()
            return ", ".join(f"`{c}`" for c in names)

        db = self.staging.conf.DB_NAME
        with self.staging.engine.begin() as con:
            con.execute(text(f"CREATE DATABASE IF NOT EXISTS {PROD_DB}"))
            for table in COPIED_TABLES:
                cols = columns(con, table)
                con.execute(text(f"DROP TABLE IF EXISTS {PROD_DB}.{table}"))
                con.execute(text(f"CREATE TABLE {PROD_DB}.{table} LIKE {db}.{table}"))
                con.execute(text(f"INSERT INTO {PROD_DB}.{table} ({cols}) SELECT {cols} FROM {db}.{table}"))
            # Only the config rows something points at. One indexed lookup per pointer column: an
            # OR across them makes MySQL scan a quarter of a million rows instead.
            cols = columns(con, "chart_configs")
            con.execute(text(f"DROP TABLE IF EXISTS {PROD_DB}.chart_configs"))
            con.execute(text(f"CREATE TABLE {PROD_DB}.chart_configs LIKE {db}.chart_configs"))
            con.execute(text(
                f"INSERT INTO {PROD_DB}.chart_configs ({cols}) SELECT {cols} FROM {db}.chart_configs WHERE id IN ("
                f"SELECT configId FROM {db}.charts UNION SELECT patchConfigId FROM {db}.charts "
                f"UNION SELECT patchConfigIdETL FROM {db}.charts WHERE patchConfigIdETL IS NOT NULL "
                f"UNION SELECT chartConfigId FROM {db}.narrative_charts "
                f"UNION SELECT patchConfigId FROM {db}.narrative_charts)"))
            cols = columns(con, "variables")
            con.execute(text(f"DROP TABLE IF EXISTS {PROD_DB}.variables"))
            con.execute(text(f"CREATE TABLE {PROD_DB}.variables LIKE {db}.variables"))
            con.execute(text(
                f"INSERT INTO {PROD_DB}.variables ({cols}) SELECT {cols} FROM {db}.variables v "
                f"WHERE EXISTS (SELECT 1 FROM {db}.chart_dimensions cd WHERE cd.variableId = v.id)"))
            # Approvals belong to staging, not to production.
            con.execute(text(f"DELETE FROM {PROD_DB}.chart_diff_approvals"))
            con.execute(text(f"DELETE FROM {PROD_DB}.chart_diff_conflicts"))

    def set_stamps(self, env: OWIDEnv, when, chart_id: int | None = None) -> None:
        self.sql(env, "UPDATE charts SET updatedAt = :w, lastEditedAt = :w WHERE id = :id",
                 w=when, id=chart_id or self.chart_id)

    def edit_in_production(self, subtitle: str, chart_id: int | None = None) -> None:
        """Someone edits the chart's subtitle in the production admin."""
        chart_id = chart_id or self.chart_id
        self.sql(self.production,
                 "UPDATE chart_configs cc JOIN charts c ON cc.id IN (c.configId, c.patchConfigId) "
                 "SET cc.config = JSON_SET(cc.config, '$.subtitle', :s), cc.updatedAt = NOW() WHERE c.id = :id",
                 s=subtitle, id=chart_id)
        self.sql(self.production,
                 "UPDATE charts SET updatedAt = NOW(), lastEditedAt = NOW(), lastEditedByUserId = :u WHERE id = :id",
                 u=self.user_id, id=chart_id)

    def revert_production(self, subtitle: str, when) -> None:
        self.sql(self.production,
                 "UPDATE chart_configs cc JOIN charts c ON cc.id IN (c.configId, c.patchConfigId) "
                 "SET cc.config = JSON_SET(cc.config, '$.subtitle', :s) WHERE c.id = :id",
                 s=subtitle, id=self.chart_id)
        self.set_stamps(self.production, when)

    def create_chart_on_staging(self) -> int:
        """Create a chart the way an ETL step's first push does."""
        variable_id = int(self.staging.read_sql(
            f"SELECT variableId FROM chart_dimensions WHERE chartId = {self.chart_id} LIMIT 1").variableId[0])
        result = self.api.upsert_chart_etl_config(
            chart_config_id=NEW_UUID,
            grapher_config={
                "title": "chart_sync_scenarios test chart",
                "subtitle": "Created by scripts/chart_sync_scenarios.py; deleted at the end of the run.",
                "slug": NEW_SLUG,
                "dimensions": [{"property": "y", "variableId": variable_id}],
            },
            catalog_path=NEW_CATALOG_PATH,
        )
        return int(result["chartId"])

    def production_etl_creates_chart(self, staging_chart_id: int) -> int:
        """Create the same chart in the production copy, as production's own ETL would after a merge.

        Same config UUID (the chart's identity), an id production mints for itself, the same ETL
        layer, and an admin patch holding only the bootstrap slug. That last part matters: copying
        staging's patch instead would erase the very difference S5 is about.
        """
        src = self.staging.read_sql(
            "SELECT configId, patchConfigId, patchConfigIdETL, etlConfigCatalogPath, isInheritanceEnabled, "
            f"forceDatapage FROM charts WHERE id = {staging_chart_id}").iloc[0]
        raw = self.staging.read_sql(f"SELECT config FROM chart_configs WHERE id = '{src.patchConfigIdETL}'").config[0]
        etl_config = json.loads(raw) if isinstance(raw, str) else raw

        with self.staging.engine.begin() as con:
            # Must differ from the staging chart's id, or the two rows match by number and this
            # stops being the situation under test. The copy was taken before this chart existed,
            # so its own max would hand out exactly the staging id.
            new_id = int(con.execute(text(
                f"SELECT GREATEST((SELECT COALESCE(MAX(id), 0) FROM {PROD_DB}.charts), "
                f"(SELECT COALESCE(MAX(id), 0) FROM {self.staging.conf.DB_NAME}.charts)) + 1")).scalar())
            assert new_id != staging_chart_id

            slug = etl_config.get("slug", NEW_SLUG)
            rendered = {**etl_config, "slug": slug, "id": new_id, "version": 1}
            patch = {k: v for k, v in
                     {"$schema": etl_config.get("$schema"), "slug": slug, "id": new_id, "version": 1}.items()
                     if v is not None}

            con.execute(text(f"DELETE FROM {PROD_DB}.charts WHERE configId = :c"), {"c": src.configId})
            for config_id, config in [(src.configId, rendered), (NEW_PATCH_UUID, patch),
                                      (src.patchConfigIdETL, etl_config)]:
                con.execute(text(f"DELETE FROM {PROD_DB}.chart_configs WHERE id = :i"), {"i": config_id})
                con.execute(text(f"INSERT INTO {PROD_DB}.chart_configs (id, config, createdAt, updatedAt) "
                                 "VALUES (:i, :c, NOW(), NOW())"), {"i": config_id, "c": json.dumps(config)})
            con.execute(text(
                f"INSERT INTO {PROD_DB}.charts (id, etlConfigCatalogPath, configId, patchConfigId, patchConfigIdETL, "
                "isInheritanceEnabled, forceDatapage, createdAt, updatedAt, lastEditedAt, lastEditedByUserId) "
                "VALUES (:id, :path, :cfg, :patch, :etl, :inh, :fd, NOW(), NOW(), NOW(), :u)"),
                {"id": new_id, "path": src.etlConfigCatalogPath, "cfg": src.configId, "patch": NEW_PATCH_UUID,
                 "etl": src.patchConfigIdETL, "inh": int(src.isInheritanceEnabled),
                 "fd": int(src.forceDatapage), "u": self.user_id})
            con.execute(text(
                f"INSERT INTO {PROD_DB}.chart_dimensions (id, `order`, variableId, property, chartId) "
                f"SELECT NULL, `order`, variableId, property, :id FROM {self.staging.conf.DB_NAME}.chart_dimensions "
                "WHERE chartId = :src"), {"id": new_id, "src": staging_chart_id})
        return new_id

    def delete_test_chart(self, chart_id: int) -> None:
        from etl.http import session as http_session

        resp = http_session.delete(f"{self.staging.admin_api}/charts/{chart_id}",
                                   headers=self.api._headers(self.user_id), timeout=(10, 120))
        print(f"  deleted the test chart {chart_id} (HTTP {resp.status_code})")
        self.sql(self.staging, "DELETE FROM chart_diff_approvals WHERE chartId = :id", id=chart_id)
        self.sql(self.staging, "DELETE FROM chart_diff_conflicts WHERE chartId = :id", id=chart_id)

    # -------------------------------------------------------------- the scenarios
    def run(self) -> bool:
        # A previous run may have died before its cleanup (the staging admin restarts, and
        # calls to it 502). The test chart would then be copied into the production stand-in
        # below, and S5 would be testing a chart that already exists there.
        leftover = self.staging.read_sql(
            f"SELECT id FROM charts WHERE configId = '{NEW_UUID}'").id.tolist()
        for chart_id in leftover:
            print(f"Removing chart {chart_id} left behind by an earlier run")
            self.delete_test_chart(int(chart_id))
        self.build_production_copy()
        created_at = self.staging.read_sql(
            "SELECT MIN(create_time) AS t FROM information_schema.tables WHERE table_schema = DATABASE()").t[0]
        # The chart's last real edit, from before this staging server existed. Restoring the
        # timestamps to it at the end is what makes the script re-runnable.
        last_edit = self.staging.read_sql(
            f"SELECT COALESCE(MAX(createdAt), (SELECT createdAt FROM charts WHERE id = {self.chart_id})) AS t "
            f"FROM chart_revisions WHERE chartId = {self.chart_id} AND createdAt < '{created_at}'").t[0]
        print(f"staging server created {created_at}; chart {self.chart_id} last edited {last_edit} before that")

        identity = self.staging.read_sql(
            f"SELECT configId, etlConfigCatalogPath FROM charts WHERE id = {self.chart_id}").iloc[0]
        self.config_uuid, self.catalog_path = identity.configId, identity.etlConfigCatalogPath

        original_config = self.api.get_chart_config(self.chart_id)
        raw = self.staging.read_sql(
            f"SELECT cc.config FROM charts c JOIN chart_configs cc ON cc.id = c.patchConfigIdETL "
            f"WHERE c.id = {self.chart_id}").config[0]
        original_etl_config = json.loads(raw) if isinstance(raw, str) else raw
        backup = BASE_DIR / f"chart_{self.chart_id}_backup.json"
        backup.write_text(json.dumps({"config": original_config, "etl_config": original_etl_config}, indent=2))

        try:
            print("\nSetup: both copies identical, untouched since the staging server was created")
            self.set_stamps(self.staging, last_edit)
            self.set_stamps(self.production, last_edit)
            staging_row, production_row = self.show("staging   ", self.staging), self.show("production", self.production)
            self.check("setup: staging chart untouched since the server was created",
                       staging_row.lastEditedAt < created_at, str(staging_row.lastEditedAt))
            self.check("setup: production chart untouched too",
                       production_row.lastEditedAt < created_at, str(production_row.lastEditedAt))
            self.check("setup: identical charts are not listed", self.diff() is None, self.describe(self.diff()))

            print("\nS0: a staging build re-pushes the ETL layer unchanged")
            self.api.upsert_chart_etl_config(chart_config_id=self.config_uuid,
                                             grapher_config=copy.deepcopy(original_etl_config),
                                             catalog_path=self.catalog_path)
            r = self.show("staging   ", self.staging)
            self.check("S0: an unchanged re-push is not a save (lastEditedAt untouched)",
                       r.lastEditedAt == last_edit, f"{r.lastEditedAt} (updatedAt moved: {r.updatedAt != last_edit})")
            self.check("S0: nothing to review after it", self.diff() is None, self.describe(self.diff()))

            print("\nS1: the chart is edited in production; this branch never touched it")
            self.edit_in_production("Edited in the production admin (S1).")
            self.show("production", self.production)
            self.check("S1: a production-only edit is not listed on an unrelated branch",
                       self.diff() is None, self.describe(self.diff()))

            print("\nS2: the branch rebuilds the chart, changing its ETL layer")
            self.revert_production(original_config.get("subtitle"), last_edit)
            rebuilt = {**copy.deepcopy(original_etl_config), "note": "Note changed by the ETL rebuild (S2)."}
            self.api.upsert_chart_etl_config(chart_config_id=self.config_uuid, grapher_config=rebuilt,
                                             catalog_path=self.catalog_path)
            self.show("staging   ", self.staging)
            d = self.diff()
            self.check("S2: control — production untouched, so it is listed without a conflict",
                       d is not None and not d.in_conflict, self.describe(d))
            self.edit_in_production("Edited in the production admin (S2).")
            d = self.diff()
            self.check("S2: production edited too, so the same chart is now in conflict",
                       d is not None and d.in_conflict, self.describe(d))

            print("\nS3: the chart is edited in the staging admin")
            self.revert_production(original_config.get("subtitle"), last_edit)
            config = self.api.get_chart_config(self.chart_id)
            config["note"] = "Note changed by hand in the staging admin (S3)."
            self.api.update_chart(self.chart_id, config, user_id=self.user_id)
            self.show("staging   ", self.staging)
            d = self.diff()
            self.check("S3: a staging-admin edit is listed, and does not conflict on its own",
                       d is not None and not d.in_conflict, self.describe(d))

            print("\nS4: approved on staging, then the chart is edited in production")
            self.approve(self.chart_id)
            d = self.diff()
            self.check("S4: the approval is recorded", d is not None and d.is_approved, self.describe(d))
            took, log = self.chart_sync()
            self.check("S4: control — while the approval holds, chart-sync would write to production",
                       took["update"], log)
            time.sleep(1.5)
            self.edit_in_production("Edited in the production admin after approval (S4).")
            d = self.diff()
            self.check("S4: the later production edit invalidates the approval",
                       d is not None and d.is_pending and d.in_conflict, self.describe(d))
            took, log = self.chart_sync()
            self.check("S4: so chart-sync leaves production alone", not took["update"] and took["pending"], log)

            print("\nS5: a chart this branch created, edited in production before chart-sync runs")
            self.new_chart_id = new_id = self.create_chart_on_staging()
            # Grapher creates an ETL chart as a draft, and publishing is only possible through the
            # admin. So publishing on staging is the ordinary workflow, and it is what makes the
            # staging copy differ from the one production's ETL will build — i.e. the thing
            # chart-sync has to carry over. No contrived hand edit is needed.
            config = self.api.get_chart_config(new_id)
            config["isPublished"] = True
            self.api.update_chart(new_id, config, user_id=self.user_id)
            print(f"  created chart {new_id} on staging and published it, as you would before approving")
            d = self.diff(new_id)
            self.check("S5: it is listed as new, since production has no such chart",
                       d is not None and d.is_new, self.describe(d))
            self.approve(new_id)
            approval = self.staging.read_sql(
                f"SELECT targetUpdatedAt FROM chart_diff_approvals WHERE chartId = {new_id} "
                "ORDER BY updatedAt DESC LIMIT 1")
            self.check("S5: its approval records no production version, having none to record",
                       len(approval) == 1 and approval.targetUpdatedAt.isna().all(),
                       str(approval.targetUpdatedAt.tolist()))

            production_id = self.production_etl_creates_chart(new_id)
            print(f"  production's ETL created it there as chart {production_id}, same UUID")
            self.check("S5: setup — the two copies have different numeric ids",
                       production_id != new_id, f"staging={new_id} production={production_id}")
            took, log = self.chart_sync(new_id)
            self.check("S5: control — with production untouched, the new chart still syncs", took["update"], log)

            time.sleep(1.5)
            self.edit_in_production("Edited in the production admin (S5).", chart_id=production_id)
            self.show("production", self.production, production_id)
            d = self.diff(new_id)
            self.check("S5: setup — matched to production's copy by UUID, not by id",
                       d is not None and d.target_chart is not None and d.target_chart.id == production_id,
                       self.describe(d))
            for label, env, cid in [("production", self.production, production_id), ("staging   ", self.staging, new_id)]:
                layer = env.read_sql("SELECT cc.config FROM charts c JOIN chart_configs cc ON cc.id = c.patchConfigId "
                                     f"WHERE c.id = {cid}").config[0]
                print(f"  {label} authored layer: {layer}")
            took, log = self.chart_sync(new_id)
            self.check("S5: chart-sync refuses, rather than overwriting the production edit",
                       took["blocked"] and not took["update"], log)
            took, log = self.chart_sync(new_id, ignore_conflicts=True)
            self.check("S5: --ignore-conflicts overrides the refusal", took["update"] and not took["blocked"], log)

            print("\nS6: corner case — a deletion on staging that normal work never reaches")
            # Only reachable on the staging server that CREATED the chart, and only if someone
            # runs chart-sync there a second time by hand. Ordinary work cannot get here: on any
            # later branch the chart carries the target's own id, so this code never runs, and a
            # deletion syncs like any other change.
            #
            # It is here because chart-sync sees only the two current states, never the history.
            # 'the target has a footnote, the source does not' is what a deletion in the source
            # and an edit in the target both look like. Refusing is the safe reading, and
            # --ignore-conflicts is how you say which one it was.
            # Start from the state after an earlier sync: the same footnote on both sides.
            config = self.api.get_chart_config(new_id)
            config["note"] = "Footnote both sides have after an earlier sync."
            self.api.update_chart(new_id, config, user_id=self.user_id)
            self.sql(self.production,
                     "UPDATE chart_configs cc JOIN charts c ON cc.id IN (c.configId, c.patchConfigId) "
                     "SET cc.config = JSON_SET(cc.config, '$.note', 'Footnote both sides have after an earlier sync.') "
                     "WHERE c.id = :id", id=production_id)
            # Now the reviewer removes it on staging.
            config = self.api.get_chart_config(new_id)
            config.pop("note", None)
            self.api.update_chart(new_id, config, user_id=self.user_id)
            for label, env, cid in [("production", self.production, production_id), ("staging   ", self.staging, new_id)]:
                layer = env.read_sql("SELECT cc.config FROM charts c JOIN chart_configs cc ON cc.id = c.patchConfigId "
                                     f"WHERE c.id = {cid}").config[0]
                print(f"  {label} authored layer: {layer}")
            # Editing the source invalidated the earlier approval, so approve again — which is what
            # a reviewer does anyway: you make the change first, then approve it.
            self.approve(new_id)
            d = self.diff(new_id)
            self.check("S6 (corner case): setup — the deletion is approved", d is not None and d.is_approved, self.describe(d))
            took, log = self.chart_sync(new_id)
            self.check("S6 (corner case): the deletion is refused, since it reads like a target edit",
                       took["blocked"] and not took["update"], log)
            took, log = self.chart_sync(new_id, ignore_conflicts=True)
            self.check("S6 (corner case): --ignore-conflicts is the way through", took["update"] and not took["blocked"], log)

        finally:
            print("\nCleanup")
            if self.new_chart_id is not None:
                try:
                    self.delete_test_chart(self.new_chart_id)
                except Exception as e:  # noqa: BLE001
                    print(f"  could not delete the test chart {self.new_chart_id}: {e}")
            try:
                self.api.upsert_chart_etl_config(chart_config_id=self.config_uuid,
                                                 grapher_config=original_etl_config,
                                                 catalog_path=self.catalog_path)
                self.api.update_chart(self.chart_id, original_config, user_id=self.user_id)
                self.sql(self.staging, "DELETE FROM chart_diff_approvals WHERE chartId = :id", id=self.chart_id)
                self.sql(self.staging, "DELETE FROM chart_diff_conflicts WHERE chartId = :id", id=self.chart_id)
                self.set_stamps(self.staging, last_edit)
                self.show("restored  ", self.staging)
                backup.unlink(missing_ok=True)
            except Exception as e:  # noqa: BLE001
                print(f"  could not restore chart {self.chart_id}: {e}\n  its original config is in {backup}")
            try:
                self.sql(self.staging, f"DROP DATABASE IF EXISTS {PROD_DB}")
                print(f"  dropped {PROD_DB}")
            except Exception as e:  # noqa: BLE001
                print(f"  could not drop {PROD_DB}: {e}")

        print("\n" + "=" * 78)
        for name, ok, detail in self.results:
            print(f"{'PASS' if ok else 'FAIL'}  {name}  [{detail}]")
        passed = sum(ok for _, ok, _ in self.results)
        print(f"\n{passed}/{len(self.results)} passed")
        return passed == len(self.results)


@click.command(name="chart-sync-scenarios", cls=click.RichCommand, help=__doc__)
@click.option("--staging", required=True, help="Staging server branch name, e.g. my-branch (not the full hostname).")
@click.option("--chart-id", type=int, default=7118, show_default=True,
              help="An ETL-authored chart on that server, used for S0 to S4.")
@click.option("--user-id", type=int, default=None,
              help="User id to attribute admin edits to. Defaults to the environment's GRAPHER_USER_ID.")
def cli(staging: str, chart_id: int, user_id: int | None) -> None:
    staging = staging.removeprefix("staging-site-")
    scenarios = Scenarios(staging, chart_id, user_id or int(OWIDEnv.from_staging(staging).conf.GRAPHER_USER_ID or 1))
    owner = scenarios.staging.read_sql(
        f"SELECT patchConfigIdETL FROM charts WHERE id = {chart_id}").patchConfigIdETL[0]
    if owner is None:
        raise click.ClickException(
            f"Chart {chart_id} on staging-site-{staging} has no ETL config layer, so it cannot exercise these "
            "scenarios. Pass --chart-id for a chart authored by an ETL step."
        )
    sys.exit(0 if scenarios.run() else 1)


if __name__ == "__main__":
    cli()
