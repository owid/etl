from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    # Load configuration from adjacent yaml file.
    config = paths.load_collection_config()

    c = paths.create_collection(
        config=config,
    )

    c.save()


#grapher/worldbank_wdi/2025-09-08/wdi/wdi#ny_gnp_pcap_pp_kd