"""Build a one-row-per-question Markdown table of the indicators a PR created, with each response
option linked to its Grapher **admin** page (staging or production).

GENERIC: nothing about the specific indicators is hardcoded. The script
  1. finds the columns the PR created      = (branch meta.yml var keys) − (master keys);
  2. resolves each column's Grapher id     from the chosen DB (staging or production);
  3. maps each column → IVS question        — loop-group columns via the branch meadow VARS_DICT
     (suffix = underscore(label) → code); custom single-question blocks are clustered by stripping a
     generic recode-prefix vocabulary, keyed by their column suffix;
  4. emits one row per question, every option linked and labelled with its real category name (from the
     variable name), aggregates first (positive before negative), Yes before No, dk/na/avg last.

Only edit the CONFIG block. TOPICS/LABELS/CODE_FOR are OPTIONAL polish (grouping, nicer labels, and an
IVS code for custom blocks whose column name doesn't contain one); with all three empty you still get a
complete flat table keyed by code (loop groups) or column-suffix (custom blocks).

    .venv/bin/python .claude/skills/add-ivs-indicators/scripts/indicator_admin_table.py
"""

import os
import re
import subprocess
from collections import Counter

import pandas as pd
from owid.catalog.core.utils import underscore
from sqlalchemy import create_engine

from etl.config import OWIDEnv, get_container_name
from etl.files import ruamel_load

# ─── CONFIG ────────────────────────────────────────────────────────────────
VERSION = "2025-06-27"
BRANCH = "data-womensrights-humanrights-crimeindicators"
ENV = "staging"  # "staging" | "production"
OUT = "ai/ivs_pr_by_topic.md"
# Once the PR is MERGED, master already contains the new columns, so the branch-vs-master diff is empty.
# Then either set BASE_REF to the merge-base / pre-merge commit, or point NEW_COLS_FILE at a saved list.
BASE_REF = "master"
NEW_COLS_FILE = ""  # e.g. "ai/ivs_pr_new_cols.txt"

TOPICS: dict[str, list[str]] = {}  # OPTIONAL grouping/order: {"Topic": [code_or_suffix, ...]}
LABELS: dict[str, str] = {}  # OPTIONAL label override: {code_or_suffix: "Nicer question label"}
CODE_FOR: dict[str, str] = {}  # OPTIONAL IVS code for a custom block: {column_suffix: "IVS_CODE"}

# Generic IVS recode-answer prefixes, only needed to split CUSTOM-block columns (loop columns split
# exactly via VARS_DICT). Longest-first stripping handles overlaps (e.g. "not_very" before "not").
ANSWER_PREFIXES = [
    "agg",
    "agree_agg",
    "disagree_agg",
    "frequently_agg",
    "not_frequently_agg",
    "strongly_agree",
    "strongly_disagree",
    "agree",
    "disagree",
    "neither",
    "neutral",
    "very_much",
    "a_great_deal",
    "to_a_certain_extent",
    "not_so_much",
    "not_at_all_frequently",
    "very_frequently",
    "quite_frequently",
    "not_frequently",
    "not_at_all",
    "not_very",
    "not_much",
    "much",
    "very",
    "quite",
    "not",
    "yes",
    "no",
    "mentioned",
    "notmentioned",
    "better_off",
    "worse_off",
    "concerned",
    "not_concerned",
    "feel_close",
    "very_close",
    "not_close_at_all",
    "not_very_close",
    "not_close",
    "close",
    "at_least_weekly",
    "less_than_weekly",
    "daily",
    "weekly",
    "monthly",
    "less_than_monthly",
    "never",
    "great_deal",
    "some",
    "none_at_all",
    "often",
    "sometimes",
    "rarely",
    "dont_know",
    "no_answer",
    "avg_score",
]
# ───────────────────────────────────────────────────────────────────────────

META = f"etl/steps/data/garden/ivs/{VERSION}/integrated_values_surveys.meta.yml"
MEADOW = f"etl/steps/data/meadow/ivs/{VERSION}/integrated_values_surveys.py"
CATALOG = f"ivs/{VERSION}/integrated_values_surveys"


def _show(ref: str, path: str) -> str:
    return subprocess.run(["git", "show", f"{ref}:{path}"], capture_output=True, text=True).stdout


def meta_keys(ref: str) -> set:
    return set(ruamel_load(_show(ref, META))["tables"]["integrated_values_surveys"]["variables"])


def vars_dict(ref: str) -> dict:
    """Extract the meadow VARS_DICT literal {code: label}."""
    m = re.search(r"VARS_DICT\s*=\s*\{.*?\n\}", _show(ref, MEADOW), re.S)
    assert m, "VARS_DICT not found in meadow step"
    ns: dict = {}
    exec(compile(m.group(), "<vars_dict>", "exec"), ns)
    return ns["VARS_DICT"]


NEW = (
    {c.strip() for c in open(NEW_COLS_FILE) if c.strip()} if NEW_COLS_FILE else meta_keys(BRANCH) - meta_keys(BASE_REF)
)
assert NEW, "no new columns — PR already merged into BASE_REF? set NEW_COLS_FILE or BASE_REF to the merge base"
VD = vars_dict(BRANCH)
CODE2LABEL = dict(VD)
SUFFIX2CODE = {underscore(label): code for code, label in VD.items()}
LOOP_SUFFIXES = sorted(SUFFIX2CODE, key=len, reverse=True)  # longest first
PREFIXES = sorted(ANSWER_PREFIXES, key=len, reverse=True)


def split(sn: str) -> tuple[str, str]:
    """→ (question_key, option_prefix). Loop-group columns resolve to the IVS code."""
    for suf in LOOP_SUFFIXES:
        if sn == suf or sn.endswith("_" + suf):
            prefix = "agg" if sn == suf else sn[: len(sn) - len(suf) - 1]
            return SUFFIX2CODE[suf], prefix
    for p in PREFIXES:
        if sn.startswith(p + "_"):
            suf = sn[len(p) + 1 :]
            return CODE_FOR.get(suf, suf), p
    return sn, sn  # single-column question (e.g. an avg_score-only index)


def label_for(qkey: str, names: list[str]) -> str:
    if qkey in LABELS:
        return LABELS[qkey]
    if qkey in CODE2LABEL:  # loop group → use the VARS_DICT label
        return CODE2LABEL[qkey]
    stems = [n.rsplit(":", 1)[0].strip() for n in names]  # strip trailing ": option"
    return Counter(stems).most_common(1)[0][0]


# ── option-link labelling + ordering ────────────────────────────────────────
# Label each option link with its real category name (so e.g. "Fairly much respect" is visible, not the
# terse recode key "some"): strip the per-question shared prefix (and a parenthetical suffix) from the
# variable name; fixed text for dk/na/avg; humanise the recode key when nothing strips cleanly.
FIXED_OPT = {"dont_know": "Don't know", "no_answer": "No answer", "avg_score": "average score"}
# Aggregate recode keys (the high/low/net rollups), and which of them are the "positive" rollup.
POS_AGG = {"agg", "agree_agg", "frequently_agg", "feel_close", "at_least_weekly", "concerned"}
NEG_AGG = {"not", "disagree_agg", "not_frequently_agg", "not_close", "less_than_weekly", "not_concerned"}


def is_agg(p: str) -> bool:
    return p == "agg" or p.endswith("_agg") or p in POS_AGG or p in NEG_AGG


def option_labels(g) -> dict:
    """prefix -> readable label, for one question's variables."""
    cat = g[~g.prefix.isin(FIXED_OPT)]
    names = list(cat.name)
    cp = os.path.commonprefix(names)
    cs = os.path.commonprefix([n[::-1] for n in names])[::-1]
    cut_p = cp[: cp.rfind(": ") + 2] if ": " in cp else (cp[: cp.rfind(" ") + 1] if " " in cp else "")
    cs_w = cs[cs.find(" ") :] if " " in cs else ""
    cut_s = cs_w if cs_w.startswith(" (") else ""  # only strip a *parenthetical* common suffix
    out = {}
    for _, r in g.iterrows():
        if r.prefix in FIXED_OPT:
            out[r.prefix] = FIXED_OPT[r.prefix]
            continue
        s = r["name"]
        if len(cut_p) >= 6 and s.startswith(cut_p):
            s = s[len(cut_p) :]
        if cut_s and s.endswith(cut_s):
            s = s[: -len(cut_s)]
        s = s.strip(" :,")
        out[r.prefix] = (
            s
            if (s and s != r["name"].strip(" :,"))
            else r.prefix.replace("_agg", " (aggregate)").replace("_", " ").strip().capitalize()
        )
    return out


def order_key(prefix: str, label: str):
    """Aggregates first (positive before negative), Yes before No, then categories; dk/na/avg last."""
    if prefix == "dont_know":
        return (5, 0, "")
    if prefix == "no_answer":
        return (6, 0, "")
    if prefix == "avg_score":
        return (7, 0, "")
    if is_agg(prefix):
        return (0, 0 if prefix in POS_AGG else 1, label)
    if prefix == "yes":
        return (2, 0, "")
    if prefix == "no":
        return (2, 1, "")
    return (3, 0, label)


# ── resolve ids + admin base (the only staging/production difference) ──
if ENV == "staging":
    container = get_container_name(BRANCH)
    engine = create_engine(
        f"mysql+pymysql://owid:@{container}.tail6e23.ts.net:3306/owid", connect_args={"connect_timeout": 10}
    )
    admin, ds_admin = f"http://{container}/admin/variables", f"http://{container}/admin/datasets"
else:  # production — reach the prod grapher DB directly over TAILSCALE (host `prod-db`, port 3306).
    # This is more reliable than .env's `127.0.0.1:3310` local SSH tunnel (often down). Credentials are
    # the live_grapher creds in .env / .env.live. ids DIFFER from staging and exist only after merge+deploy.
    from sqlalchemy.engine import URL

    c = OWIDEnv.from_env_file(".env").conf  # live_grapher user/pass (same in .env.live)
    engine = create_engine(
        URL.create(
            "mysql+pymysql", username=c.DB_USER, password=c.DB_PASS, host="prod-db", port=3306, database=c.DB_NAME
        ),
        connect_args={"connect_timeout": 8},
    )
    admin, ds_admin = "https://admin.owid.io/admin/variables", "https://admin.owid.io/admin/datasets"

v = pd.read_sql(
    f"SELECT v.id, v.shortName, v.name FROM variables v JOIN datasets d ON v.datasetId=d.id "
    f"WHERE d.catalogPath='{CATALOG}'",
    engine,
)
v = v[v.shortName.isin(NEW)].copy()
assert len(v) == len(NEW), f"matched {len(v)}/{len(NEW)} — dataset published to {ENV}? (prod ids differ from staging)"
v["qkey"], v["prefix"] = zip(*v.shortName.map(split))

rows = {}  # qkey -> (question label, option links)
for qkey, g in v.groupby("qkey"):
    lab = option_labels(g)  # prefix -> readable option label
    items = sorted((order_key(r.prefix, lab[r.prefix]), lab[r.prefix], r.id) for _, r in g.iterrows())
    links = " · ".join(f"[{label}]({admin}/{vid})" for _, label, vid in items)
    rows[qkey] = (label_for(qkey, g.name.tolist()), links)

ds_id = pd.read_sql(f"SELECT id FROM datasets WHERE catalogPath='{CATALOG}'", engine)["id"].iloc[0]
lines = [
    f"[Dataset]({ds_admin}/{ds_id})",
    "",
    f"Each link in the table points to that response option's {ENV} variable.",
    "",
]


def emit(title: str, keys: list[str]) -> None:
    if title:
        lines.append(f"## {title}")
    lines.extend(["", "| IVS code | Question | Option variables |", "|---|---|---|"])
    lines.extend(f"| `{k}` | {rows[k][0]} | {rows[k][1]} |" for k in keys if k in rows)
    lines.append("")


if TOPICS:
    for topic, keys in TOPICS.items():
        emit(topic, keys)
    leftover = sorted(k for k in rows if k not in {x for ks in TOPICS.values() for x in ks})
    if leftover:
        emit("Other", leftover)
else:
    emit("", sorted(rows))

with open(OUT, "w") as f:
    f.write("\n".join(lines))
print(f"wrote {OUT}: {len(v)} indicators across {len(rows)} questions (env={ENV})")
