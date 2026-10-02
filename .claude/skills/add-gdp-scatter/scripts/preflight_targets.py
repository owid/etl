"""Pre-flight an add-gdp-scatter input table before anything is written.

`apply_scatter_defaults.py` does not validate that a target CAN take a scatter view, and
it reports `OK` on rows that silently do nothing (see SKILL.md, "Picking targets"). This
script checks the four conditions up front and prints the verdict per row.

Reads the same JSON list as the applier from stdin — `{chart_admin_url,
target_chart_admin_url, gdp_source}` — and writes the runnable subset to stdout as JSON,
so the two compose:

    cat rows.json | preflight_targets.py --emit | apply_scatter_defaults.py

Without `--emit`, only the human-readable report is printed (stdout stays clean of JSON).
The report goes to stderr so it stays visible when piping.

Configs are read from the PUBLIC DATASETTE, i.e. production — which is the state a
staging DB was cloned from. That is deliberate: it reflects the pre-run baseline even if
an earlier run already touched staging.
"""

import argparse
import json
import sys
import urllib.parse

from etl.http import session

# Any GDP-per-capita variable, current or superseded: a source's non-GDP indicator is
# whatever is NOT in here. Kept deliberately broad so an older source still resolves.
GDP_IDS = {1294305, 1204826, 900793, 1108541}
STACKED = {"StackedArea", "StackedBar", "StackedDiscreteBar"}
CHART_ID_SEP = "/charts/"


def chart_id_from_url(url: str) -> int:
    return int(url.split(CHART_ID_SEP)[1].split("/")[0])


def datasette(sql: str) -> list[dict]:
    url = "https://datasette-public.owid.io/owid.json?" + urllib.parse.urlencode({"sql": sql, "_shape": "array"})
    r = session.get(url, timeout=90)
    r.raise_for_status()
    return r.json()


def log(msg: str) -> None:
    print(msg, file=sys.stderr)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--emit", action="store_true", help="print the runnable rows as JSON on stdout")
    args = parser.parse_args()

    rows = json.load(sys.stdin)
    pairs = [(chart_id_from_url(r["chart_admin_url"]), chart_id_from_url(r["target_chart_admin_url"]), r) for r in rows]

    ids = sorted({i for src, tgt, _ in pairs for i in (src, tgt)})
    cfgs = {
        r["id"]: r
        for r in datasette(
            "select c.id, cc.slug, json_extract_string(cc.config,'$.chartTypes') as types, "
            "json_extract_string(cc.config,'$.dimensions') as dims, "
            "json_extract_string(cc.config,'$.isPublished') as pub "
            f"from charts c join chart_configs cc on cc.id=c.configId where c.id in ({','.join(map(str, ids))})"
        )
    }

    def dims_of(chart_id: int, prop: str) -> list[int]:
        return [d["variableId"] for d in json.loads(cfgs[chart_id]["dims"] or "[]") if d["property"] == prop]

    runnable, blocked = [], []
    log(f"{'src':>6} {'tgt':>6}  {'target chartTypes':<28} {'y#':>3}  verdict")
    log("-" * 110)

    for src, tgt, row in pairs:
        if src not in cfgs or tgt not in cfgs:
            missing = [c for c in (src, tgt) if c not in cfgs]
            blocked.append((src, tgt, f"not found in the production mirror: {missing}"))
            log(f"{src:>6} {tgt:>6}  {'?':<28} {'?':>3}  BLOCKED: not in production mirror {missing}")
            continue

        types_raw = cfgs[tgt]["types"]
        types = json.loads(types_raw or "[]")
        ty = dims_of(tgt, "y")
        non_gdp = [v for v in dims_of(src, "y") + dims_of(src, "x") if v not in GDP_IDS]

        problems = []
        if "ScatterPlot" in types:
            problems.append("target is already a ScatterPlot (only one x dimension exists)")
        if set(types) & STACKED:
            problems.append(f"target is stacked-family {sorted(set(types) & STACKED)}")
        if len(ty) != 1:
            problems.append(f"target has {len(ty)} y indicators, need exactly 1")
        if cfgs[tgt]["pub"] != "true":
            problems.append("target is not published")
        if not set(ty) & set(non_gdp):
            problems.append("target's y is not the source's non-GDP indicator")

        shown = types_raw or "(default line/bar)"
        if problems:
            blocked.append((src, tgt, "; ".join(problems)))
            log(f"{src:>6} {tgt:>6}  {shown[:28]:<28} {len(ty):>3}  BLOCKED: {'; '.join(problems)}")
            log(f"{'':>14}source non-GDP indicator {non_gdp}, target y {ty} ({cfgs[tgt]['slug']})")
        else:
            runnable.append(row)
            log(f"{src:>6} {tgt:>6}  {shown[:28]:<28} {len(ty):>3}  OK")

    log(f"\nrunnable: {len(runnable)}   blocked: {len(blocked)}")
    if blocked:
        log("Drop the blocked rows — the applier would report OK and change nothing useful.")

    if args.emit:
        json.dump(runnable, sys.stdout)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
