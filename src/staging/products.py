import pandas as pd
from pathlib import Path
from ..utils.staging_base import (
    load_csv,
    standardize_columns,
    normalize_empty_strings,
    enforce_dtypes
)

DTYPE_MAP = {
    "product_id": "string",
    "product_category_name": "string",
    "product_name_lenght": "float64",
    "product_description_lenght": "float64",
    "product_photos_qty": "float64",
    "product_weight_g": "float64",
    "product_length_cm": "float64",
    "product_height_cm": "float64",
    "product_width_cm": "float64"
}

def transform_products(path: Path) -> pd.DataFrame:
    df = load_csv(path)
    df = standardize_columns(df)
    df = normalize_empty_strings(df)
    df = enforce_dtypes(df, DTYPE_MAP)

    return df