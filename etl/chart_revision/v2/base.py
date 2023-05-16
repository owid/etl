from typing import Any, Dict


class ChartUpdater:
    def run(self, config: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError
