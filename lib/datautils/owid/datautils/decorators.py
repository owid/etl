"""Library decorators."""


import functools
import tempfile
from typing import Any, Callable, Optional

from owid.datautils.s3 import S3
from owid.datautils.web import download_file_from_url


def enable_file_download(path_arg_name: Optional[str] = None) -> Callable[[Any], Any]:
    """Enable downloading of files from URLs."""
    # Download options, add them as needed (value: str, key: Tuple[str])
    prefixes = {
        "url": (
            "http://",
            "https://",
        ),
        "s3": ("s3://",),
    }
    # Get list of prefixes as a flat tuple
    prefixes_flat = tuple(prefix for prefixes_list in prefixes.values() for prefix in prefixes_list)

    def _enable_file_download(func: Callable[[Any], Any]) -> Callable[[Any], Any]:
        @functools.wraps(func)
        def wrapper_download(*args: Any, **kwargs: Any) -> Any:
            # Get path to file
            _used_args = False
            if args:
                args = list(args)  # type: ignore
                path = args[0]
                _used_args = True
            else:
                path = kwargs.get(path_arg_name)  # type: ignore
                if path is None:
                    raise ValueError(f"Filename was not found in args or kwargs ({path_arg_name}!")
            # Check if download is needed and download
            path = str(path)
            if path.startswith(prefixes_flat):  # Download from URL and run function
                with tempfile.NamedTemporaryFile() as temp_file:
                    # Download file from URL
                    if path.startswith(prefixes["url"]):
                        download_file_from_url(path, temp_file.name)  # TODO: Add custom args here
                    # Download file from S3 (need credentials)
                    elif path.startswith(prefixes["s3"]):
                        S3().download_from_s3(path, temp_file.name, quiet=True)  # TODO: Add custom args here

                    # Modify args/kwargs
                    if _used_args:
                        args[0] = temp_file.name  # type: ignore
                    else:
                        kwargs[path_arg_name] = temp_file.name  # type: ignore
                    # Call function
                    return func(*args, **kwargs)
            else:  # Run function on local file
                return func(*args, **kwargs)

        return wrapper_download

    return _enable_file_download
