"""Load a garden dataset and create a grapher dataset."""

from owid.catalog import processing as pr

from etl.helpers import PathFinder, create_dataset, grapher_checks

paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # load garden WHO data
    ds_garden = paths.load_dataset("autopsy")
    tb_who = ds_garden["autopsy"].reset_index()
    assert all(tb_who["sex"].drop_duplicates() == "ALL")
    tb_who = tb_who.drop(columns="sex").rename(columns={"value": "autopsy_rate"})

    # load Paratz paper snapshot
    snap_paratz = paths.load_snapshot("paratz.csv")
    tb_paratz = snap_paratz.read_csv()

    # drop Paratz rows whose countries are already covered by WHO
    tb_paratz = tb_paratz[~tb_paratz["country"].isin(tb_who["country"])]

    # combine
    tb = pr.concat([tb_paratz, tb_who]).format(["country", "year"], short_name=paths.short_name)

    # save grapher dataset (inherit metadata from upstream WHO garden dataset)
    ds_grapher = create_dataset(dest_dir, tables=[tb], default_metadata=ds_garden.metadata)
    ds_grapher.update_metadata(paths.metadata_path)
    grapher_checks(ds_grapher)
    ds_grapher.save()
