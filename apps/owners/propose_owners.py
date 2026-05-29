"""Propose `owners:` assignments for active garden datasets from git history.

Writes a Markdown report to ``ai/owners-proposal.md`` that lists the top
candidate owners for each active garden dataset, ranked by how often each
OWID team member has touched the dataset's `.py` / `.meta.yml` files in
non-sweep, non-formatting commits.

The script is **read-only** — it does not edit any YAML. Use the output to
drive per-author/per-namespace follow-up PRs that actually write `owners:`
into each dataset's `.meta.yml`.

Run::

    .venv/bin/python apps/owners/propose_owners.py
"""

from __future__ import annotations

import subprocess
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path

from etl.dag_helpers import load_dag
from etl.owners import OWID_DATA_TEAM, resolve_owner
from etl.paths import BASE_DIR

# A commit that touches more distinct datasets than this is treated as a
# sweep (formatting pass, repo-wide refactor, chart-sync archive, schema
# migration) and excluded from per-dataset attribution. Genuine data work
# almost always lands in PRs that touch 1–3 datasets, so the cutoff sits
# well below batch-update territory.
SWEEP_DATASET_CUTOFF = 10

# Commit subjects matching any of these substrings are treated as
# mechanical and dropped, even if the commit only touches one dataset.
# Useful for single-touch sweeps that the dataset-count filter misses.
_SWEEP_SUBJECT_SUBSTRINGS: tuple[str, ...] = (
    "chart-sync",
    "chart sync",
    "archive ",
    "lint",
    "format",
    "ruff",
    "typing",
    "type check",
    "typecheck",
    "migrate metadata",
    "bulk update",
    "auto-archive",
    "backport",  # owidbot-style legacy imports, attributed to the person who pressed the button
    "safe_types",
    "fasttrack",
    "schema migration",
    # The migrator typically just runs the script; not the data owner.
    "migrate",
    # NOTE: "fix " was here, but it was too aggressive — it killed focused
    # `🐛 Fix detect changed datasets, and update bullfighting laws` etc.
    # The conditional 🐛 prefix filter + SHA blocklist + `📊 Fix measles
    # pipeline`-style explicit SHAs cover the real maintenance cases.
    # Cross-dataset mechanical sweeps we keep seeing.
    "code practices",  # "upgrade code good practices", "remove old code practices"
    "indicator tags",  # the pablor / lucas sweeps
    "remove line breaks",
    "html -> markdown",
)

# Commit subjects starting with any of these tokens are treated as
# maintenance and dropped. Covers both the rendered emoji and the
# GitHub shortcode forms we see in older commits (`:bug:` / `:honeybee:`).
_SWEEP_SUBJECT_PREFIXES: tuple[str, ...] = (
    "🐛",
    "🐝",
    ":bug:",
    ":honeybee:",
)

# One-off commits the heuristic can't catch — typically infra work that
# happened to touch a few step files (e.g. perf experiments, CI tweaks,
# template fixes). Maintained by hand from the per-author triage lists
# under ai/triage_<author>.md.
_COMMIT_SHA_BLOCKLIST: frozenset[str] = frozenset(
    {
        # Source → origin / legacy migration sweep (24)
        "4169de5992",  # 📊 Migrate Core Econ snapshots from meta.source to meta.origin
        "971a3db84b",  # 📊 Migrate Source to Origin in open_numbers and covid steps
        "d0181de0d5",  # 📊 Migrate UN SDG snapshot/meadow/garden/grapher to origins
        "b523577fb6",  # 📊 Migrate commodity_prices to origins
        "73c9e9fc80",  # 📊 Migrate flu_elderly to origins
        "74a372379a",  # 📊 Migrate flu_vaccine_policy to origins
        "918a0dbb5c",  # 📊 Migrate gapminder/2023-03-31/population to origins
        "a82d4529a0",  # 📊 Migrate global_wellbeing to origins
        "67b0d16765",  # 📊 Migrate habitat loss
        "82bf72661e",  # 📊 Migrate hcctad to origins
        "7e1b222220",  # 📊 Migrate health_pharma_market to origins
        "d1bb1f9503",  # 📊 Migrate microprocessor_trend to origins
        "353996aa0c",  # 📊 Migrate semiconductors_cset to origins
        "cac247032e",  # 📊 Migrate us_patents to origins
        "699a68246a",  # 📊 Migrate wgm_mental_health to origins
        "60134b2a87",  # 📊 Migrate 8 flat-legacy snapshot DVCs to origin
        "f95b1afac5",  # 📊 Migrate excess_mortality snapshots to meta.origin
        "a6801d9628",  # 📊 Migrate Historical employment and output by sector to ETL
        "d04a54cc65",  # Migrate legacy OWID datasets to ETL
        "4e5cf776a3",  # 🔨 Migrate Walden to Snapshots
        "600b7d14c7",  # :bar_chart: switch WDI from sources to origins
        "49cb2f0d08",  # :tada: add Origin as a replacement for Source
        "2521b1bb9e",  # :hammer: Add origins to population dataset
        "5ce6e48c51",  # Revert ":hammer: Add origins to population dataset"
        # Framework / walkthrough / dummy (18)
        "86193225d9",  # refactor: remove Names alias and remove calls to catalog.find
        "80ed049f52",  # enhance(helpers): add helper function create_dataset
        "57c9d0d715",  # feature(etl): improve metadata ergonomics
        "92c1894bae",  # fix(data): align metadata with dataset paths
        "64308e9c87",  # :hammer: remove publisher_source field
        "720896ba5e",  # :hammer: refactor walkthrough
        "9ddd901d04",  # :hammer: remove logging from walkthrough
        "823507edbe",  # :hammer: update walkthrough to use latest best practices
        "ec8210c268",  # :sparkles: improve walkthrough experience
        "aac735f553",  # :hammer: put back default_metadata in walkthrough
        "5ea07ecff2",  # Enhance(walkthrough): commit dummy dataset
        "61261b6557",  # fix(walkthrough): add some fields
        "02aad80be5",  # :sparkles: small metadata changes (dummy)
        "6dcb16d7c5",  # :tada: final metadata renames (dummy)
        "4015e52bd8",  # :tada: Add origins as a default to walkthrough
        "2730b892fd",  # :tada: add functions load_dataset and load_snapshot
        "999db955ca",  # 🔨 Remove .ipynb step support
        "246cab8d42",  # ✨ Add Copilot instructions
        # Perf / CI / infra / lib (26)
        "48f2fa0631",  # :hammer: Use groupby(..., observed=True) by default
        "01dba80576",  # :sparkles: compute checksums from ingredients only
        "4616136473",  # 📊 Reduce CI log clutter
        "773d6745f0",  # 📊 Reduce build log noise
        "0e3cf2f894",  # 🔨 Deprecate countries_regions.csv
        "3fa2f56e78",  # :sparkles: Less verbose logs & fix warnings
        "9477e5c97c",  # :sparkles: Jinja whitespaces and newlines
        "e4c5d58215",  # :sparkles: Speed-up grapher upsert
        "09b4b91fa7",  # ✨ Check regions file for duplicates
        "3c0b9bcde7",  # :sparkles: Improve data-diff
        "e49e43ae95",  # :sparkles: Use real users on staging servers
        "aa0ffcbbdd",  # ✨ Convert dtypes to safe types
        "f99f4cffc4",  # :hammer: exclude country_profile from datadiff
        "be655c2341",  # ✨ Simplify GBD metadata with Jinja macros
        "f2a0e89463",  # ✨ Add weighted average to geo
        "ee7acda5f3",  # :hammer: use categoricals for GHE
        "44d111c887",  # :tada: automatic wildfires update (bot wiring)
        "3148ecfdf8",  # :tada: strict YAML metadata schema validation
        "dea5f2f15d",  # 📊 Optimize GBD repack + structlog timestamps
        "e9edd96da1",  # 📊 Optimize historical_poverty step performance
        "2b0bafb82a",  # 🔨 Unify usage of SUBSET and GRAPHER_FILTER
        "fa9e70c45a",  # 🔨 Move WDI metadata fetching from garden to snapshot
        "d2c643c9ea",  # 🔨 Read WDI rich metadata from WDISeries.csv
        "0a1af53d5c",  # 📊 Fix explicit un_wpp loading in population helper callers
        "91d46825ef",  # 🔨🤖 Update YEAR_MAX_EXPECTED to 2026 in hmd_stmf
        "cb06c522ef",  # 🎉 Make dimensions more prominent (infra disguised as 🎉)
        # The owners-field PR itself
        "b9dedc64cb",  # 🎉🤖 Add owners field to dataset metadata
        # Reverts
        "2904282ff9",  # Revert ":bug: hotfix problems with escaping <1%"
        # Backstop one-offs (also caught by "migrate"/"fix " subject filters)
        "f2d13126f0",  # :bar_chart: migrate fertility rate and GDP to ETL
        "cfdd4f4222",  # 📊 Fix measles pipeline
        "4d97c42dbd",  # 🔨 upgrade-code to stop using `dest_dir` (#5132)
        "d5b117d6b1",  # Chore general maintenance (#5256)
        # Multi-dataset sweeps surfaced from random multi-author samples
        "e1337b3b91",  # 💄 remove old code practices (#5829)
        "b34a568ce5",  # ✨ indicator tags (lucas) (#1869)
        "9e4a2a6ba2",  # ✨ metadata: remove line breaks (#3401)
        "a3060bd9b0",  # ✨ indicator tags (pablor) (#1870)
        "7df6c34ca8",  # 🔨 Improve add region aggregates (#2047)
        "d94f3575ca",  # 🔨 owid-catalog: deprecate find-like methods (#5478)
        "abcaa477e8",  # 🔨 upgrade code good practices (#3762)
        "4143dfe6ca",  # refactor: Use geo from etl.data_helpers instead of owid.datautils
        "c1212e5c80",  # 🔨 decouple code (#4064)
        "f9c233bf15",  # 💄 change dataset title (#3768)
        "9695c6f49a",  # ✨ correct deprecated code (#3760)
        "37541934ed",  # 🔨 load_dependency: remove (#2884)
        "38419fa827",  # 📊 Small improvements on sources and origins for various datasets (#3438)
        "21960c27a0",  # ✨ metadata: HTML -> markdown
        "13321a8ed8",  # 🔨 archive datasets (#3756) — 46-file sweep
        # Mojmír's "I touched this once, not the owner" commits
        "9efb8cf541",  # :bar_chart: real commodity price index
        "9aede0030c",  # 🐛 Fix homicide dataset
        "1015964158",  # 🐛 Use correct UN dependency in WHO Mortality DB
        "4b14c09223",  # :bug: Fix colonical dates
        "b61a18807e",  # :bar_chart: Create Colonial Dates Dataset (rebased) — actually @paarriagadap's PR
        "12b3323d7f",  # :bug: Helper function geo.add_population_to_dataframe (framework)
        "30680bdd5f",  # 🐛 🐛 Fix le_sex_gap_age_contribution garden step
        "5f33a46505",  # 📊 Update ERT dataset (v16) — autobot run, not owner
        "4fbec79ab7",  # 📊 Update democracy/2025-06-02/fh — autobot run
        "0de76e0617",  # 🐛 Add support for new COVID variants in sequence data
        "0a5fba40b4",  # :bug: fix duplicate values in covid sequences
        "248aeb6e1f",  # 📊 Upgrade ty to 0.0.26 and pandas-stubs to 2.2.3 (tooling sweep)
        # Single-dataset Mojmír framework fixes that pass the conditional 🐛/🐝
        # filter (touch only 1 active dataset) but still aren't ownership work.
        "a1f6d85b5c",  # :honeybee: drop all missing values (WVS framework fix)
        "45dcf2c7c6",  # :bug: fix groupby with function keywords (WVS)
        "f9b478c7d3",  # :bug: hotfix reserved count keyword in groupby.transform (WVS)
        "1018780ec6",  # :bug: fix age group condition in GHE metadata
        "00469b1aab",  # :bug: fix age variable reference in GHE metadata
        "87efd6ac73",  # :bar_chart: Update ghe — autobot run, not owner
        "dd41032518",  # :bug: remove breakpoint (education_lee_lee)
        "206833873b",  # :bug: fix origin of education dataset
        # Round of explicit additions from the multi-owner audit.
        # Cross-dataset grammar / spelling / wording sweeps.
        "d57b2c34ea",  # :lipstick: fix imports (Lars)
        "c1c0d5ed1d",  # :bug: Fix sources of UN SDG (#2053)
        "3702dccad0",  # 🐛 Fix global terrorism database dataset (#5474)
        "116abcc949",  # 💄 Fix "by World Bank" → "by the World Bank" in attributions (#5925)
        "5c5fb179e3",  # Fix grammar: 'people that' → 'people who' (#5927)
        "21fee682a3",  # 💄🤖 Fix grammar: 'children that' → 'children who', fix 'Estiamted' typo
        "10cae448db",  # Fix metadata wording in Eurostat and Gallup AI indicators (#5955)
        "2903af27e5",  # Update phrasing from 'that are' to 'who are' in meta.yml
        "73e71d8df8",  # Fix typo in Pew Research Center attribution
        "338c3fd0c8",  # Standardize 'data centre' → 'data center' spelling
        "88ef8e7534",  # Capitalize 'World Happiness Report' in metadata
        # Framework / schema / lib sweeps that touched a few datasets.
        "6f29eb925d",  # 🔨 remove dest_dir from recent steps (#4417)
        "bc7572b978",  # 💄 drop `dest_dir` in 2025 steps (#4634)
        "c9679ea3a2",  # 📊 demography: Fix population metadata (#5475)
        "da1868cbef",  # 📊 Fix typos in explorers, mdims, charts and posts, found by codespell (#5203)
        "5d4f894adb",  # 📊 Fix inspector issues (#5471)
        "ca3d23b7bd",  # ✨ improve faust of demography explorer (#5915)
        "eead44f615",  # 🎉 owid-catalog v1.0.0rc0 pre-release (#5373)
        "692620f6ea",  # fix typo in GWP metadata (#5589) — Marcel
        "2c978bbb1d",  # 🔨 update dataset-schema with new colorScale settings (#4969) — Marcel
        "866c00dcdb",  # 🐛 fix CLI name references: etl -> etl run (#2313)
        "6d492c82ce",  # 📊 Fix harmonization of Sudan (former) (#5234)
        "1f201acecb",  # 🐝 clean datasets with same name (#3963)
        "194a4f252d",  # 🐛 fix hiv/aids lowercase (#3900)
        # Mojmír single-dataset framework / data-quality fixes that aren't ownership.
        "2dcc5b42e4",  # :bug: fix jinja for covid sequences
        "4f663e764d",  # :bug: Fix jinja template for COVID reporting
        "bcf0c24d95",  # :bug: gbd-risk: Add missing percentages (#3262)
        "1c4b557aab",  # :honeybee: remove slug field from grapher_config JSON schema
        "d3f37664ed",  # :bug: fix jinja in antimicrobial
        "fc84e0fd62",  # :bug: fix indentation (invasive_species)
        "ca2e59d7ab",  # :bug: Add Côte d'Ivoire to expected missing countries (covid)
        "a93fbd3a6d",  # :bug: remove us patent metadata
        "fec379f14e",  # :bug: replace copy_metadata_from by copy_metadata (#1494)
        "1379302ca7",  # :bar_chart: Combined origin in population table (#1528)
        "7b7c0902d5",  # :bar_chart: add Fiji to excess mortality and remove years == 0 (#1772)
        "1d5cb48058",  # :bug: bump expected year in XM checks (#2194)
        "41dc8b26a8",  # :bug: bump up expected year in hmd
        "b9899267c0",  # :bug: bump validation year for hmd_stmf
        "c1bc12f5ff",  # :bug: hotfix assertion in hmd_stmf
        "aacb9bd245",  # :bug: fix situation when year is not in the index (LGBT)
        "8a431b38a6",  # :bug: hotfix problems with escaping <1% (plastic_fate_regions)
        "3db1a97601",  # :bug: hotfix covid failing download
        "f329a72d4a",  # data(health): fix description of Wellcome Global Monitor 2020 (#1050)
        "766efbe2c9",  # :bug: fix cow.meta.yml missing if
        "1a90a04953",  # :bug: fix bouthoul_carrere yaml
        "b5a089503d",  # :bug: fix missing variable in jinja (mars)
        "237308bb98",  # 🐝 Update pyproj (#4774)
        "3497064737",  # 🐛 Fix pyproj bug in UCDP (#4914)
        "93f2edbc00",  # :bug: Add retries to ucdp
        "d2856a6413",  # 🐛 Fix ValueError: Invalid value supplied 'WktVersion.WKT2_2019' (#5021)
        "c76b872d8a",  # :bar_chart: convert age-group to ordinal variable (un_wpp_most)
        "dd0aa20b4c",  # :bug: Fix jinja template (un_wpp)
        "7b473c6cc7",  # 🐛🤖 measles: drop new filter_esp column from CDC source
        "b61ded692f",  # 🐝 Rollback weighted average from geo (#5015)
    }
)

# Manual overrides for steps where the git-log heuristic gets the
# ownership wrong (or where there's no git history yet). The list is
# treated as the final answer — the heuristic counts are discarded for
# these steps. First entry = accountable owner.
_OWNER_OVERRIDES: dict[str, list[str]] = {
    # Veronika is the long-term owner; Mojmír's drive-by edits shouldn't
    # promote him to primary.
    "data://garden/artificial_intelligence/2026-04-24/energy_ai": ["Veronika Samborska"],
    # Pablo is the de-facto owner; everyone has touched regions over time.
    "data://garden/regions/2023-01-01/regions": ["Pablo Rosado"],
    # New step from this PR — no git history yet, attribute to Pablo.
    "data://garden/faostat/2026-02-25/additional_variables": ["Pablo Rosado"],
    # Bastian originated the nuclear-weapons family; Pablo did recent maintenance.
    "data://garden/war/2026-05-13/nuclear_weapons_inventories": ["Bastian Herre", "Pablo Rosado"],
    "data://garden/war/2026-05-13/nuclear_weapons_tests": ["Bastian Herre", "Pablo Rosado"],
    "data://garden/war/2026-05-13/nuclear_weapons_treaties": ["Bastian Herre", "Pablo Rosado"],
}

# How many top candidates to surface per dataset in the proposal.
TOP_N = 3

OUTPUT_PATH = BASE_DIR / "ai" / "owners-proposal.md"
GARDEN_STEP_DIR = BASE_DIR / "etl" / "steps" / "data" / "garden"


@dataclass
class _Commit:
    sha: str
    email: str
    name: str
    subject: str
    files: list[str]


def _active_garden_datasets() -> dict[str, list[Path]]:
    """Map each active garden step URI to its source files on disk (relative to repo)."""
    dag = load_dag()
    out: dict[str, list[Path]] = {}
    for step in dag:
        if not (step.startswith("data://garden/") or step.startswith("data-private://garden/")):
            continue
        # e.g. data://garden/biodiversity/2024-01-25/cherry_blossom
        suffix = step.split("//", 1)[1]  # garden/biodiversity/.../cherry_blossom
        rel_base = Path("etl/steps/data") / suffix
        files = [rel_base.with_suffix(ext) for ext in (".py", ".meta.yml")]
        existing = [p for p in files if (BASE_DIR / p).exists()]
        if existing:
            out[step] = existing
    return out


def _git_log_for_files(files: list[Path]) -> list[_Commit]:
    """Run a single `git log` over all the given files and return parsed commits."""
    if not files:
        return []
    # %H, %ae, %an, %s separated by \t; --no-merges to skip merge commits;
    # --name-only to emit changed file paths.
    pretty = "--pretty=format:--COMMIT--%n%H%x09%ae%x09%an%x09%s"
    cmd = ["git", "log", "--no-merges", "--name-only", pretty, "--"] + [str(p) for p in files]
    result = subprocess.run(cmd, cwd=BASE_DIR, capture_output=True, text=True, check=True)
    commits: list[_Commit] = []
    for block in result.stdout.split("--COMMIT--\n"):
        block = block.strip()
        if not block:
            continue
        lines = block.splitlines()
        header = lines[0]
        try:
            sha, email, name, subject = header.split("\t", 3)
        except ValueError:
            continue
        commits.append(
            _Commit(sha=sha, email=email, name=name, subject=subject, files=[line for line in lines[1:] if line])
        )
    return commits


def _is_sweep_subject(subject: str) -> bool:
    """Subject-substring filter only. Emoji prefix is checked separately
    because we want to allow focused single-dataset 🐛 fixes through."""
    s = subject.lower()
    return any(needle in s for needle in _SWEEP_SUBJECT_SUBSTRINGS)


def _has_maintenance_prefix(subject: str) -> bool:
    stripped = subject.lstrip()
    return any(stripped.startswith(prefix) for prefix in _SWEEP_SUBJECT_PREFIXES)


def _step_namespace(step: str) -> str:
    """Return the namespace component of a step URI (`data://garden/<ns>/<ver>/<short>`)."""
    return step.split("/")[3]


def _drop_sweeps(commits: list[_Commit], file_to_step: dict[str, str]) -> list[_Commit]:
    """Drop commits that look mechanical:

    - SHA in the explicit blocklist
    - sweep-flavoured subject substring (chart-sync, migrate, ...)
    - touching more than SWEEP_DATASET_CUTOFF distinct active datasets,
      UNLESS they all sit in the same namespace (e.g. a FAOSTAT or
      emissions bulk update — that's focused work, not a cross-repo sweep)
    - 🐛/🐝/`:bug:`/`:honeybee:` prefix AND touching > 1 active dataset
      (single-dataset 🐛 fixes survive — see bullfighting_laws etc.)
    """
    kept: list[_Commit] = []
    for c in commits:
        if c.sha in _COMMIT_SHA_BLOCKLIST or c.sha[:10] in _COMMIT_SHA_BLOCKLIST:
            continue
        if _is_sweep_subject(c.subject):
            continue
        touched_steps = {file_to_step[f] for f in c.files if f in file_to_step}
        if len(touched_steps) > SWEEP_DATASET_CUTOFF:
            namespaces = {_step_namespace(s) for s in touched_steps}
            if len(namespaces) > 1:
                continue
            # Single-namespace bulk update — allow the commit through.
        if _has_maintenance_prefix(c.subject) and len(touched_steps) > 1:
            continue
        kept.append(c)
    return kept


def _aggregate(commits: list[_Commit], file_to_step: dict[str, str]) -> dict[str, Counter[str]]:
    """For each step, count non-sweep commits per resolved OWID owner.

    A commit that touches both the `.py` and `.meta.yml` of one dataset counts
    once for that dataset (not twice).
    """
    per_step: dict[str, Counter[str]] = defaultdict(Counter)
    for c in commits:
        owner = resolve_owner(c.name) or resolve_owner(c.email)
        if owner is None:
            continue  # bot, ex-employee not in mapping, or unknown contributor
        touched_steps = {file_to_step[f] for f in c.files if f in file_to_step}
        for step in touched_steps:
            per_step[step][owner] += 1
    # Hard overrides win over the heuristic. Encode order via descending
    # synthetic counts so the markdown ranking matches the override list.
    for step, owners in _OWNER_OVERRIDES.items():
        per_step[step] = Counter({name: len(owners) - i for i, name in enumerate(owners)})
    return per_step


def _write_markdown(per_step: dict[str, Counter[str]], all_steps: dict[str, list[Path]]) -> None:
    by_namespace: dict[str, list[str]] = defaultdict(list)
    for step in all_steps:
        # data://garden/<namespace>/<version>/<short_name>
        ns = step.split("/")[3]
        by_namespace[ns].append(step)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        f.write("# Proposed dataset owners\n\n")
        f.write(
            f"Top candidates per active garden dataset, ranked by non-sweep "
            f"commits to the dataset's `.py` / `.meta.yml` files. "
            f"Sweep commits (touching >{SWEEP_DATASET_CUTOFF} datasets) and commits by "
            f"non-team contributors are excluded.\n\n"
        )
        for ns in sorted(by_namespace):
            f.write(f"## {ns}\n\n")
            f.write("| dataset | top candidates (commits) |\n")
            f.write("|---|---|\n")
            for step in sorted(by_namespace[ns]):
                counts = per_step.get(step, Counter())
                if not counts:
                    candidates = "_no team contributions found_"
                else:
                    top = counts.most_common(TOP_N)
                    candidates = ", ".join(f"{name} ({n})" for name, n in top)
                short = step.rsplit("/", 1)[-1]
                version = step.split("/")[-2]
                f.write(f"| `{version}/{short}` | {candidates} |\n")
            f.write("\n")

        f.write("---\n\n")
        f.write("Reference: canonical names = " + ", ".join(OWID_DATA_TEAM) + "\n")


def main() -> None:
    steps = _active_garden_datasets()
    file_to_step: dict[str, str] = {str(p): step for step, files in steps.items() for p in files}
    all_files = sorted({p for files in steps.values() for p in files})
    commits = _git_log_for_files(all_files)
    commits = _drop_sweeps(commits, file_to_step)
    per_step = _aggregate(commits, file_to_step)
    _write_markdown(per_step, steps)
    print(f"Wrote proposal for {len(steps)} active garden datasets to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
