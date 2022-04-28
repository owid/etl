import requests
import pandas as pd
import time
from pywebio import start_server
from pywebio.input import input, FLOAT, TEXT
from pywebio.output import (
    put_text,
    put_table,
    put_markdown,
    put_button,
    use_scope,
)
from pywebio.session import set_env, download
from pywebio.pin import *


def _df_to_array(df):
    return [df.columns] + df.to_numpy().tolist()


def _search(term):
    url = "http://localhost:8000/search"
    resp = requests.get(url, params={"term": term})
    df = pd.DataFrame(resp.json())
    # put_text()

    if df.empty:
        return [["No results"]]

    df = df.drop(columns=["fields"])

    df = df[
        [
            "score",
            "table",
            "version",
            "namespace",
            "path",
            "format",
            "short_name",
            "description",
            "title",
            "table_name",
        ]
    ]

    return _df_to_array(df)


def bmi():
    set_env(output_animation=False)
    put_markdown("""# OWID Data Catalog""")
    put_markdown(
        """## WARNING: Full-text search currently uses all columns which is a nonsense"""
    )
    put_markdown("""## Search term""")
    put_input("search_term")

    put_markdown("## Results")
    while True:
        search_term = pin_wait_change("search_term")
        with use_scope("md", clear=True):
            # put_markdown(search_term["value"], sanitize=False)
            table = _search(search_term["value"])
            put_table(table)

            # get table
            if len(table) >= 2:
                table_name = table[1][-1]

                url = f"http://localhost:8000/table/{table_name}.feather"
                t = time.time()
                df = pd.read_feather(url)
                duration = time.time() - t
                # NOTE: read_feather adds overhead
                put_markdown(
                    f"""## Table {table_name} preview

                Dataframe shape: {df.shape}
                Dataframe size: {df.memory_usage().sum() / 1024 / 1024:.2f} MB
                Latency of pd.read_feather: {duration:.3f} s
                """
                )
                put_table(_df_to_array(df.head(20)))

    # term = input("Search term", type=TEXT)
    # term = "sdg 16"

    # url = "http://localhost:8000/search"
    # resp = requests.get(url, params={"term": term})
    # print(resp.json())
    # df = pd.DataFrame(resp.json())
    # # put_text()

    # df = df.drop(columns=["fields"])

    # df = df[
    #     [
    #         "score",
    #         "table",
    #         "version",
    #         "namespace",
    #         "path",
    #         "format",
    #         "short_name",
    #         "description",
    #         "title",
    #         "table_name",
    #     ]
    # ]

    # put_table([df.columns] + df.to_numpy().tolist())


if __name__ == "__main__":
    start_server(bmi, port=8001, debug=True)
