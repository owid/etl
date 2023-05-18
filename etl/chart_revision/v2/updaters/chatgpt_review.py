from typing import Any, Dict

from etl.chart_revision.v2.base import ChartUpdater


class ChartUpdaterGPTSuggestions(ChartUpdater):
    def run(self, config: Dict[str, Any]):
        """"""
        print("NOT IMPLEMENTED: This should access the config and update it with the GPT suggestions.")
