from pathlib import Path

from click.testing import CliRunner

from etl.prune import prune


def test_prune(tmp_path: Path) -> None:
    # create dag file
    dag_file = tmp_path / "dag.yml"
    dag_file.write_text("steps:\n  data://garden/owid/latest/covid:")

    feather_file = tmp_path / "data/garden/owid/latest/todelete/todelete.feather"
    feather_file.parent.mkdir(exist_ok=True, parents=True)
    feather_file.write_text("")

    assert feather_file.exists()

    # NOTE: this is pain to work with, I should have separated CLI from the functionality
    # and test it like regular python function
    runner = CliRunner()
    result = runner.invoke(
        prune,
        [
            "--dag-path",
            dag_file,  # type: ignore
            "--data-dir",
            tmp_path / "data",  # type: ignore
        ],
    )

    assert result.exit_code == 0
    assert not feather_file.exists()
