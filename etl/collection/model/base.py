import json
from pathlib import Path
from typing import Union

from owid.catalog.meta import MetaBase


class MDIMBase(MetaBase):
    def save_file(self, filename: Union[str, Path]) -> None:
        filename = Path(filename).as_posix()
        with open(filename, "w") as ostream:
            json.dump(self.to_dict(), ostream, indent=2, default=str)
