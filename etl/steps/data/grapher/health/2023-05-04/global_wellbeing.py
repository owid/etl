"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    ds_garden = paths.load_dataset("global_wellbeing")
    tb_questions = ds_garden.read("global_wellbeing", reset_index=False)
    tb_index = ds_garden.read("global_wellbeing_index", reset_index=False)

    ds_grapher = paths.create_dataset(
        tables=[tb_questions, tb_index], default_metadata=ds_garden.metadata
    )
    ds_grapher.save()
