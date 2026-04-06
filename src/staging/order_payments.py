import pandas as pd
from pathlib import Path
from ..utils.staging_base import (
    load_csv,
    standardize_columns,
    normalize_empty_strings,
    enforce_dtypes
)

DTYPE_MAP = {
    "order_id": "string",
    "payment_sequential": "int64",
    "payment_type": "string",
    "payment_installments": "int64",
    "payment_value": "float64"
}

def transform_order_payments(path: Path) -> pd.DataFrame:
    df = load_csv(path)
    df = standardize_columns(df)
    df = normalize_empty_strings(df)
    df = enforce_dtypes(df, DTYPE_MAP)

    return df