"""Show indicators in datasets from database.

The idea is to quickly prototype a better way to show indicators in datasets.

TODO: only works for ETL-based datasets.
"""

from collections import defaultdict

import pandas as pd
import streamlit as st

from etl.config import OWID_ENV
from etl.grapher.io import load_variables_in_dataset


class IndicatorWithDimensions:
    def __init__(self, indicators):
        """Object with all indicator-dimensions.

        indicators: List of Variable objects. They should all belong to the same indicator.
        """
        self.indicators = indicators
        self.df = self.create_df()
        self.dimensions = self.get_dimensions()

    def create_df(self):
        data = []
        for indicator in self.indicators:
            # Prepare data
            data_ = {
                "variable": indicator,
            }
            # Add dimensions, if any
            if "filters" in indicator.dimensions:
                for f in indicator.dimensions["filters"]:
                    data_[f["name"]] = f["value"]

            # Append to main list
            data.append(data_)

        # Build dataframe
        df = pd.DataFrame.from_records(data)
        columns_index = [col for col in df.columns if col != "variable"]
        df = df.set_index(columns_index)

        return df

    def get_dimensions(self):
        dimensions = {level: self.df.index.get_level_values(level).unique().tolist() for level in self.df.index.names}
        return dimensions


# DATASET_ID = 6869
DATASET_ID = 6813

# Load variables for given dataset
indicators = load_variables_in_dataset(dataset_id=[DATASET_ID])

# Prepare data
data_with_dim = defaultdict(list)
data_no_dim = []
for indicator in indicators:
    # Add dimensions, if any
    if indicator.dimensions is not None:
        short_name = indicator.dimensions["originalShortName"]
        table = indicator.catalogPath
        assert isinstance(indicator.catalogPath, str), f"`catalogPath` is empty for variable {indicator.id}"
        table = indicator.catalogPath.split("#")[0]
        key = f"{table}#{short_name}"
        data_with_dim[key].append(indicator)
    # Does not have dimensions
    else:
        assert isinstance(indicator.catalogPath, str), f"`catalogPath` is empty for variable {indicator.id}"
        key = indicator.catalogPath
        data_no_dim.append(indicator)

# Prepare objects
data = {}
for key, vars in data_with_dim.items():
    data[key] = IndicatorWithDimensions(vars)


########################################
# RENDER
# TODO: show title based on selection
########################################
# st.write(st.session_state)
for key, indicator in data.items():
    with st.container(border=False):
        s = st.container()

        dim_value = None
        for dim, options in indicator.dimensions.items():
            key_pills = f"dataset_pills_{key}_{dim}"
            st.pills(
                dim,
                options,
                key=key_pills,
            )

            dim_value = st.session_state.get(key_pills)

        if dim_value is not None:
            var = indicator.df.loc[dim_value].item()
            name = var.name
            iid = var.id
            s.markdown(f"[**{name}**]({OWID_ENV.indicator_admin_site(iid)})")
            s.caption(var.catalogPath)
        else:
            s.markdown(f"**{key}**")
            s.caption(f"{key}")
    st.divider()
