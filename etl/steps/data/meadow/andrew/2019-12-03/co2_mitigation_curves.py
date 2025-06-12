from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# naming conventions
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("co2_mitigation_curves.start")

    # Load raw dataset on mitigation curves for 2 Celsius.
    snap = paths.load_snapshot("co2_mitigation_curves_2celsius.csv")
    tb_2celsius = snap.read()

    # Load raw dataset on mitigation curves for 1.5 Celsius.
    snap = paths.load_snapshot("co2_mitigation_curves_1p5celsius.csv")
    tb_1p5celsius = snap.read()

    tb_2celsius = tb_2celsius.underscore()
    tb_1p5celsius = tb_1p5celsius.underscore()

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds = create_dataset(dest_dir, tables=[tb_2celsius, tb_1p5celsius], default_metadata=snap.metadata)

    # finally save the dataset
    ds.save()

    log.info("co2_mitigation_curves.end")
