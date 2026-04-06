import pandas as pd
from pathlib import Path
from ..utils.staging_base import (
    load_csv,
    standardize_columns,
    normalize_empty_strings,
    convert_to_datetime,
    enforce_dtypes
)

DATE_COLUMNS = [
    "shipping_limit_date"
]

DTYPE_MAP = {
    "order_id": "string",
    "order_item_id": "int64",
    "product_id": "string",
    "seller_id": "string",
    "price": "float64",
    "freight_value": "float64"
}

def transform_order_items(path: Path) -> pd.DataFrame:
    df = load_csv(path)
    df = standardize_columns(df)
    df = normalize_empty_strings(df)
    df = convert_to_datetime(df, DATE_COLUMNS)
    df = enforce_dtypes(df, DTYPE_MAP)

    return df