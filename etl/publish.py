#
#  publish.py
#  etl
#

import click
from sh import aws

from owid.catalog import LocalCatalog

from etl import config
from etl.command import DATA_DIR


class CannotPublish(Exception):
    pass


@click.command()
def publish() -> None:
    """
    Publish the generated data catalog to S3.
    """
    LocalCatalog(DATA_DIR).reindex()
    sync_to_s3()


def sync_to_s3() -> None:
    aws(
        "s3",
        "--endpoint-url",
        config.S3_ENDPOINT_URL,
        "sync",
        "--acl=public-read",
        DATA_DIR.as_posix(),
        f"s3://{config.S3_BUCKET}/",
        _fg=True,
    )


if __name__ == "__main__":
    publish()
