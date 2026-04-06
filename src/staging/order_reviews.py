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
    "review_creation_date",
    "review_answer_timestamp"
]

DTYPE_MAP = {
    "review_id": "string",
    "order_id": "string",
    "review_score": "int64",
    "review_comment_title": "string",
    "review_comment_message": "string"
}

def transform_order_reviews(path: Path) -> pd.DataFrame:
    df = load_csv(path)
    df = standardize_columns(df)
    df = normalize_empty_strings(df)
    df = convert_to_datetime(df, DATE_COLUMNS)
    df = enforce_dtypes(df, DTYPE_MAP)

    return df