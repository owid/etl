"""Helper tool to create map brackets for all indicators in an indicator-based explorer.

"""
import numpy as np
import pandas as pd
import streamlit as st

# PAGE CONFIG
st.set_page_config(
    page_title="Wizard: Explorer editor",
    page_icon="🪄",
)
st.title(":material/explore: Explorer Editor")


with st.container(border=True):
    st.subheader("IDs to Paths")
    st.markdown("Migrate all references to indicator IDs for their corresponding indicator paths.")

    uploaded_files = st.file_uploader(
        label="Upload Explorer config file",
        type=["csv", "tsv"],
    )


class ExplorerConfig:
    def __init__(self, df: pd.DataFrame):
        self.sanity_checks(df)
        self._df_init = df
        self._col_top_level = df.columns[0]
        self._df_proc = self._process_df(df, df.columns[0])
        # Config parts (header, graphers, columns
        self.header = self._get_header()
        self.graphers = self._get_df_graphers()
        self.columns = self._get_df_columns()

    def sanity_checks(self, df: pd.DataFrame):
        assert not df.isna().all().any(), "Some columns are all NaNs. Remove them with .dropna(axis=1, how='all')"
        keys = [
            "graphers",
            "explorerTitle",
        ]
        for key in keys:
            assert key in set(df[df.columns[0]].unique()), f"No {key} property in explorer config!"

    def _process_df(self, df: pd.DataFrame, col_top_level):
        df = df.reset_index(drop=True)
        df[col_top_level] = df[col_top_level].ffill()
        return df

    def _get_header(self):
        header = self._df_proc.loc[~self._df_proc[self._col_top_level].isin(["graphers", "columns"])]
        header = header.dropna(axis=1, how="all")
        assert header.shape[1] == 2, "Header of explorer should only have two columns! Please review"
        header = header.set_index(self._col_top_level).squeeze().to_dict()
        return header

    def _get_df_graphers(self):
        return self._get_df_nested("graphers")

    def _get_df_columns(self):
        return self._get_df_nested("columns")

    def _get_df_nested(self, keyword: str) -> pd.DataFrame:
        # Keep relevant rows
        df = self._df_proc.loc[self._df_proc[self._col_top_level].isin([keyword])]
        # Remove first column, and first row
        df = df.drop(columns=[self._col_top_level]).dropna(axis=0, how="all").reset_index(drop=True)
        # Set column headers
        df, df.columns = df[1:], df.iloc[0]
        # Remove unnecessary columns
        df = df.dropna(axis=1, how="all")
        return df

    def _adapt_df_nested(self, df: pd.DataFrame, keyword: str):
        headers = pd.DataFrame([df.columns.values], columns=df.columns)
        df = pd.concat([headers, df], ignore_index=True)
        df.columns = range(1, df.shape[1] + 1)

        # Add empty row
        df = self._add_top_empty_row(df)

        # Add top-level property name
        df[0] = keyword
        df.loc[1:, 0] = np.nan

        # Order columns
        df = df.sort_index(axis=1)

        return df

    def _add_top_empty_row(self, df: pd.DataFrame):
        empty_row = pd.DataFrame([[np.nan] * len(df.columns)], columns=df.columns)
        df = pd.concat([empty_row, df], ignore_index=True)
        return df

    @property
    def as_df(self):
        # Convert header to dataframe
        df_headers = pd.DataFrame.from_dict([self.header]).T.reset_index()
        df_headers.columns = [0, 1]
        df_headers = self._add_top_empty_row(df_headers)
        # Adapt graphers
        df_graphers = self._adapt_df_nested(self.graphers, "graphers")
        # Adapt columns
        df_columns = self._adapt_df_nested(self.columns, "columns")

        df = pd.concat(
            [
                df_headers,
                df_graphers,
                df_columns,
            ],
            ignore_index=True,
        )
        return df

    def to_tsv(self, filename: str, **kwargs):
        if not filename.endswith(".tsv"):
            raise ValueError("filename should end with 'tsv'!")
        self.as_df.to_csv(
            filename,
            sep="\t",
            index=False,
            header=False,
            **kwargs,
        )

    def to_csv(self, filename: str, **kwargs):
        self.as_df.to_csv(
            filename,
            index=False,
            header=False,
            **kwargs,
        )
