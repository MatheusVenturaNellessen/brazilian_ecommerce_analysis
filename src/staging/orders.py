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
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date"
]

DTYPE_MAP = {
    "order_id": "string",
    "customer_id": "string",
    "order_status": "string"
}

def transform_orders(path: Path) -> pd.DataFrame:
    df = load_csv(path)
    df = standardize_columns(df)
    df = normalize_empty_strings(df)
    df = convert_to_datetime(df, DATE_COLUMNS)
    df = enforce_dtypes(df, DTYPE_MAP)

    return df