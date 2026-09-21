"""Guardrail: no two ETL steps claim the same chart.

A chart's identity in grapher is its config UUID (`charts.configId`), and a chart config
file declares it as `chart_config_id`. If two files declare the same one, both steps own
that chart: each build overwrites the other's config, and `charts.etlConfigCatalogPath`
flips to whichever ran last. Nothing errors, so the only symptom is a live chart quietly
changing title depending on build order.

There is no legitimate reason for two files to share a UUID, and it is an easy mistake to
make, since copying an existing config file copies its identity along with everything
else. Grapher cannot catch it either: each push is a valid API call on its own. So it has
to be caught here.
"""

import re
from collections import defaultdict
from pathlib import Path

from etl.paths import STEP_DIR

# Matches the declaration in a chart config file, quoted or not. Deliberately a text scan
# rather than a YAML parse: config files may carry Jinja, and a parse failure elsewhere
# shouldn't take this check down with it.
_CHART_CONFIG_ID_RE = re.compile(r"^\s*chart_config_id:\s*[\"']?([0-9a-fA-F-]{36})", re.MULTILINE)


def _declared_chart_config_ids() -> dict[str, list[str]]:
    """Map each declared chart config UUID to the files declaring it."""
    by_uuid: dict[str, list[str]] = defaultdict(list)
    for path in Path(STEP_DIR).glob("**/*.yml"):
        for match in _CHART_CONFIG_ID_RE.finditer(path.read_text()):
            # UUIDs are case-insensitive, and both cases occur in the wild, so compare
            # them in one canonical case rather than as written.
            by_uuid[match.group(1).lower()].append(str(path.relative_to(STEP_DIR)))
    return by_uuid


def test_chart_config_ids_are_unique():
    duplicates = {uuid: files for uuid, files in _declared_chart_config_ids().items() if len(files) > 1}
    assert not duplicates, "Several steps declare the same chart_config_id, so each would overwrite the others: " + (
        "; ".join(f"{uuid} in {', '.join(files)}" for uuid, files in sorted(duplicates.items()))
    )
