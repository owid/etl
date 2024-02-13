"""This script should be manually adapted and executed on the event of an update of the Maddison Project Database.

"""

import argparse
from pathlib import Path

from owid.walden import Dataset


def main() -> None:
    metadata = Dataset.from_yaml(Path(__file__).parent / "ggdc_maddison.meta.yml")

    # upload the local file to Walden's cache
    dataset = Dataset.download_and_create(metadata)
    # upload it to S3
    dataset.upload(public=True)
    # update PUBLIC walden index with metadata
    dataset.save()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    args = parser.parse_args()
    main()
