"""Old name of `finalize_sketch.py`, kept so older sketches' own instructions still run.

Sketches scaffolded before the rename point their docstring at this path, and they live in gitignored
directories no commit can update. This forwards every argument to `finalize_sketch.py`; use that name
in anything new.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from finalize_sketch import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
