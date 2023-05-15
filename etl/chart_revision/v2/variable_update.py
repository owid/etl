"""Chart revision transform functions when there are updates on variables.

These functions are used when there are updates on variables. They are used in the chart revision process.
"""


from typing import Dict, Any


def update_config()
def update_config_map(config: Dict[str, Any], variable_mapping: Dict[int, int], variable_meta: Dict[str, Any], variable_id_default_for_map: int) -> Dict[str, Any]:
    """Update map config.

    Parameters
    ----------
    variable_id_default_for_map : int
        ID of the first variable in the old chart. Usually found at config["dimensions"][0]["variableId"]
    config : Dict[str, Any]
        Configuration of the chart. It is assumed that no changed has occured under the property `map`.
    variable_mapping : Dict[int, int]
        Mapping from old to new variable IDs.
    variable_meta : Dict[str, Any]
        Variable metadata. Includes min and max years.

    Returns
    -------
    Dict[str, Any]
        Updated chart configuration.
    """
    # Proceed only if chart uses map
    if config['hasMapTab']:
        print("chart uses map")
        # Get map_variable_id
        map_var_id = config["map"].get("variableId", variable_id_default_for_map)  # chart.config["dimensions"][0]["variableId"]
        # Proceed only if variable ID used for map is in variable_mapping (i.e. needs update)
        if map_var_id in variable_mapping:
            # Get and set new map variable ID in the chart config
            map_var_id_new = variable_mapping[map_var_id]
            config["map"]["variableId"] = map_var_id_new
            # Get year ranges from old and new variables
            year_range_old = [variable_meta[map_var_id]["minYear"], variable_meta[map_var_id]["maxYear"]]
            year_range_new = [variable_meta[map_var_id_new]["minYear"], variable_meta[map_var_id_new]["maxYear"]]
            # Set year slider to new value based on new variable's year range
            # If set to latest (in numbers), change to 'latest'. If set to earliet, set to new earliest value (in numbers)
            current_time = config["map"]["time"]
            if (current_time == max(year_range_old)):
                config["map"]["time"] = "latest"
            elif current_time == max(year_range_old):
                config["map"]["time"] = min(year_range_new)
    return config
