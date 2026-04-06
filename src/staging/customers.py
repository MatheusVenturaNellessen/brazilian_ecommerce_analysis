import pandas as pd
from pathlib import Path
from ..utils.staging_base import (
    load_csv,
    standardize_columns,
    normalize_empty_strings,
    enforce_dtypes
)

DTYPE_MAP = {
    "customer_id": "string",
    "customer_unique_id": "string",
    "customer_zip_code_prefix": "string",
    "customer_city": "string",
    "customer_state": "string"
}

def transform_customers(path: Path) -> pd.DataFrame:
    df = load_csv(path)
    df = standardize_columns(df)
    df = normalize_empty_strings(df)
    df = enforce_dtypes(df, DTYPE_MAP)
    
    return df