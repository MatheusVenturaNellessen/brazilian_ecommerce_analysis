import pandas as pd
from pathlib import Path

def load_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(
        path,
        sep=",",
        encoding="UTF-8",
        low_memory=False
    )

def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(r"[^\w]+", "_", regex=True)
        .str.strip("_") 
    )
    
    return df

def normalize_empty_strings(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    
    df = df.replace(r'^\s*$', pd.NA, regex=True)
    
    return df

def convert_to_datetime(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    df = df.copy()
    
    for col in columns:
        df[col] = pd.to_datetime(df[col], errors="coerce")
    
    return df

def enforce_dtypes(df: pd.DataFrame, dtype_map: dict) -> pd.DataFrame:
    df = df.copy()

    for col, dtype in dtype_map.items():
        if "int" in dtype or "float" in dtype:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        else:
            df[col] = df[col].astype(dtype)

    return df

def save_parquet(df: pd.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)