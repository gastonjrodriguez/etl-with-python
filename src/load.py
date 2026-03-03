from pathlib import Path
import pandas as pd
from src.config import OUTPUT_DIR, SAVE_CSV, SAVE_PARQUET

# helper to save to .csv and .parquet

def save_to_format(df: pd.DataFrame, name: str, output_dir: Path) -> None:


    if SAVE_CSV:
        csv_path = output_dir / f'{name}.csv'
        df.to_csv(csv_path, index=False)

    if SAVE_PARQUET:
        parquet_path = output_dir / f'{name}.parquet'
        df.to_parquet(parquet_path, index=False)


# wrapper
def run_load(dfs: dict[str, pd.DataFrame], analytics: dict[str, pd.DataFrame]) -> None:

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    all_data = {**dfs, **analytics}
    # replaces:
    # all_data = dfs.copy()
    # all_data.update(analytics)

    for name, df in all_data.items():
        save_to_format(df, name, OUTPUT_DIR)

    
    print('Successfully saved.')






