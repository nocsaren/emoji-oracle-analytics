from __future__ import annotations

import json
from typing import Any, Mapping, MutableMapping, Optional

import pandas as pd
from config.logging import get_logger

logger = get_logger(__name__)


def _is_missing(value: Any) -> bool:
    if value is None:
        return True

    # Treat empty containers/arrays as "missing" for our flattening use-cases.
    if isinstance(value, (list, tuple, set, dict)):
        return len(value) == 0

    size = getattr(value, "size", None)
    if isinstance(size, int) and size == 0:
        return True

    try:
        is_na = pd.isna(value)
    except Exception:
        return False

    # pd.isna on scalars returns a bool; on array-likes it returns an array/Series.
    if isinstance(is_na, bool):
        return is_na
    if getattr(is_na, "shape", None) == ():
        return bool(is_na)
    return False


def _parse_json_if_str(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def _coerce_mapping(value: Any) -> Mapping[str, Any]:
    if _is_missing(value):
        return {}
    parsed = _parse_json_if_str(value)
    return parsed if isinstance(parsed, dict) else {}


def _update_with_prefixed_dict(
    flat: MutableMapping[str, Any],
    prefix: str,
    mapping: Mapping[str, Any],
) -> None:
    flat.update({f"{prefix}.{k}": v for k, v in mapping.items()})


def flatten_dataframe(df: pd.DataFrame, context: Optional[dict] = None) -> pd.DataFrame:
    """
    Flattens a DataFrame by expanding nested dictionaries in each row.
    
    Args:
    - df: The input pandas DataFrame with potential nested dictionary columns.
    
    Returns:
    - A new pandas DataFrame with flattened structure.
    """
    _ = context
    records = [flatten_row(row.to_dict()) for _, row in df.iterrows()]
    return pd.DataFrame.from_records(records)



def _firebase_param_value(value_dict: Mapping[str, Any]) -> Any:
    # Prioritize types: string > int > float > double
    for key in ("string_value", "int_value", "float_value", "double_value"):
        if key in value_dict and value_dict[key] is not None:
            return value_dict[key]
    return None


def flatten_extract_params(params_list: Any) -> dict[str, Any]:
    """
    Converts a list of Firebase-style param dicts into a flat {key: value} dict.
    Prioritizes value types: string > int > float > double.
    Example input: [{'key': 'foo', 'value': {'string_value': 'bar'}}]
    Returns: {'foo': 'bar'}    
    """
    result = {}

    try: 
        if isinstance(params_list, list):
            for param in params_list:
                if not isinstance(param, dict):
                    continue
                key = param.get("key")
                if not key:
                    continue
                val_dict = param.get("value")
                if not isinstance(val_dict, dict):
                    val_dict = {}

                result[key] = _firebase_param_value(val_dict)
        return result
    except Exception:
        logger.exception("Error flattening params")
        return {}


def flatten_nested_column(
    row: Mapping[str, Any],
    col_name: str,
    flat: MutableMapping[str, Any],
    default_keys: Optional[list[str]] = None,
) -> None:
    """
    Flattens a nested column in the row if it's a dictionary. If the data is missing
    (None or NaN), it handles it by updating with `None` for default keys.
    
    Args:
    - row: The row of data (as a pandas Series)
    - col_name: The name of the column to flatten
    - flat: The dictionary where flattened data will be updated
    - default_keys: A list of keys to set `None` if the column data is missing (optional)
    
    Returns:
    - None: This function updates the `flat` dictionary in place.
    """
    # Get the column value
    col_data = row.get(col_name, None)
    
    # Check if the data is a dictionary
    try:
        if isinstance(col_data, dict):
            _update_with_prefixed_dict(flat, col_name, col_data)
            return

        if _is_missing(col_data):
            if default_keys is not None:
                flat.update({f"{col_name}.{k}": None for k in default_keys})
            else:
                flat[col_name] = None
    except Exception:
        logger.exception("Error flattening column %s", col_name)
        if default_keys is not None:
            flat.update({f"{col_name}.{k}": None for k in default_keys})
        else:
            flat[col_name] = None


_BASE_FIELDS: tuple[str, ...] = (
    "event_date",
    "event_timestamp",
    "event_name",
    "event_previous_timestamp",
    "event_value_in_usd",
    "event_bundle_sequence_id",
    "event_server_timestamp_offset",
    "user_id",
    "user_pseudo_id",
    "user_first_touch_timestamp",
    "stream_id",
    "platform",
    "is_active_user",
    "batch_event_index",
    "batch_page_id",
    "batch_ordering_id",
)


def flatten_row(row: Mapping[str, Any]) -> dict[str, Any]:
    flat: dict[str, Any] = {field: row.get(field) for field in _BASE_FIELDS}

    device = _coerce_mapping(row.get("device"))
    geo = _coerce_mapping(row.get("geo"))
    app_info = _coerce_mapping(row.get("app_info"))
    traffic_source = _coerce_mapping(row.get("traffic_source"))
    user_ltv = _coerce_mapping(row.get("user_ltv"))
    privacy_info = _coerce_mapping(row.get("privacy_info"))

    event_params = flatten_extract_params(row.get("event_params", []))
    _update_with_prefixed_dict(flat, "event_params", event_params)

    user_props = flatten_extract_params(row.get("user_properties", []))
    _update_with_prefixed_dict(flat, "user", user_props)

    _update_with_prefixed_dict(flat, "privacy_info", privacy_info)
    _update_with_prefixed_dict(flat, "user_ltv", user_ltv)
    _update_with_prefixed_dict(flat, "device", device)
    _update_with_prefixed_dict(flat, "geo", geo)
    _update_with_prefixed_dict(flat, "app_info", app_info)
    _update_with_prefixed_dict(flat, "traffic_source", traffic_source)

    flatten_nested_column(row, "event_dimensions", flat, default_keys=None)
    flatten_nested_column(row, "ecommerce", flat, default_keys=None)
    flatten_nested_column(row, "items", flat, default_keys=None)
    flatten_nested_column(row, "collected_traffic_source", flat, default_keys=None)

    item_params = flatten_extract_params(row.get("item_params", []))
    _update_with_prefixed_dict(flat, "item_params", item_params)

    return flat