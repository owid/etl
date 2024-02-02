import json
from typing import Any, List


class MockResponse:
    def __init__(self, json_data: Any, status_code: int):
        self.json_data = json_data
        self.status_code = status_code

    def iter_content(self, chunk_size: int) -> List[bytes]:
        _ = chunk_size
        return [json.dumps(self.json_data).encode()]
