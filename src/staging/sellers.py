import pandas as pd
from pathlib import Path
from ..utils.staging_base import (
    load_csv,
    standardize_columns,
    normalize_empty_strings,
    enforce_dtypes
)

DTYPE_MAP = {
    "seller_id": "string",
    "seller_zip_code_prefix": "string",
    "seller_city": "string",
    "seller_state": "string",
}

def transform_sellers(path: Path) -> pd.DataFrame:
    df = load_csv(path)
    df = standardize_columns(df)
    df = normalize_empty_strings(df)
    df = enforce_dtypes(df, DTYPE_MAP)
    
    return df