from typing import Any, Dict

from structlog import get_logger

from etl.chart_revision.v2.base import ChartUpdater

log = get_logger()


class ChartUpdaterFASTTReduce(ChartUpdater):
    def run(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Update chart config."""
        if "data" in config:
            log.info("fast_reduce: Removing `data` field from FASTT.")
            del config["data"]
        return config
