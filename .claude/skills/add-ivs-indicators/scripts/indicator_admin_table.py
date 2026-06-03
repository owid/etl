"""Build a one-row-per-question Markdown table of the indicators a PR created, with each response
option linked to its Grapher **admin** page (staging or production).

GENERIC: nothing about the specific indicators is hardcoded. The script
  1. finds the columns the PR created      = (branch meta.yml var keys) − (master keys);
  2. resolves each column's Grapher id     from the chosen DB (staging or production);
  3. maps each column → IVS question        — loop-group columns via the branch meadow VARS_DICT
     (suffix = underscore(label) → code); custom single-question blocks are clustered by stripping a
     generic recode-prefix vocabulary, keyed by their column suffix;
  4. emits one row per question with every option ([agree], [dont_know], [avg_score], *_agg …) linked.

Only edit the CONFIG block. TOPICS/LABELS/CODE_FOR are OPTIONAL polish (grouping, nicer labels, and an
IVS code for custom blocks whose column name doesn't contain one); with all three empty you still get a
complete flat table keyed by code (loop groups) or column-suffix (custom blocks).

    .venv/bin/python .claude/skills/add-ivs-indicators/scripts/indicator_admin_table.py
"""

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


NEW = meta_keys(BRANCH) - meta_keys("master")
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


# ── resolve ids + admin base (the only staging/production difference) ──
if ENV == "staging":
    container = get_container_name(BRANCH)
    engine = create_engine(
        f"mysql+pymysql://owid:@{container}.tail6e23.ts.net:3306/owid", connect_args={"connect_timeout": 10}
    )
    admin, ds_admin = f"http://{container}/admin/variables", f"http://{container}/admin/datasets"
else:  # production — needs prod DB creds; ids DIFFER from staging; only after merge + re-publish
    env = OWIDEnv()  # or OWIDEnv.from_env_file(".env.prod")
    engine, admin, ds_admin = env.engine, env.indicators_admin_site, env.datasets_admin_site

v = pd.read_sql(
    f"SELECT v.id, v.shortName, v.name FROM variables v JOIN datasets d ON v.datasetId=d.id "
    f"WHERE d.catalogPath='{CATALOG}'",
    engine,
)
v = v[v.shortName.isin(NEW)].copy()
assert len(v) == len(NEW), f"matched {len(v)}/{len(NEW)} — dataset published to {ENV}? (prod ids differ from staging)"
v["qkey"], v["prefix"] = zip(*v.shortName.map(split))
v["k"] = v.prefix.map(lambda p: {"dont_know": 90, "no_answer": 91, "avg_score": 92}.get(p, 50))

rows = {}  # qkey -> (label, links)
for qkey, g in v.groupby("qkey"):
    g = g.sort_values(["k", "prefix"])
    links = " · ".join(f"[{r.prefix}]({admin}/{r.id})" for _, r in g.iterrows())
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
