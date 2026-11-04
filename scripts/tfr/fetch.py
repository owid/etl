"""Download a source file once and keep it.

Source files live under data/, which is not in the repository, so anyone starting fresh needs the
downloads to happen on their own. Anything that can be fetched without a login goes through here;
files that need a browser, a form or a hand-built export are documented per country instead.
"""

import os
import subprocess

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"


def fetch(url, path, insecure=False):
    """Return `path`, downloading `url` into it first if it is not already there."""
    if os.path.exists(path) and os.path.getsize(path) > 0:
        return path
    os.makedirs(os.path.dirname(path), exist_ok=True)
    cmd = ["curl", "-sL", "--fail", "-A", UA, "-m", "600", "-o", path, url]
    if insecure:
        cmd.insert(1, "-k")                            # some offices serve a broken chain
    subprocess.run(cmd, check=True)
    return path
