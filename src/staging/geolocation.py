import pandas as pd
from pathlib import Path
from ..utils.staging_base import (
    load_csv,
    standardize_columns,
    normalize_empty_strings,
    enforce_dtypes
)

DTYPE_MAP = {
    "geolocation_zip_code_prefix": "string",
    "geolocation_lat": "float64",
    "geolocation_lng": "float64",
    "geolocation_city": "string",
    "geolocation_state": "string",
}

def transform_geolocation(path: Path) -> pd.DataFrame:
    df = load_csv(path)
    df = standardize_columns(df)
    df = normalize_empty_strings(df)
    df = enforce_dtypes(df, DTYPE_MAP)
    
    return df