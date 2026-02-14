import os
import pandas as pd
from load import load_zhvi, load_zori

# --- File paths ---
ZHVI_FILE = os.path.join('data', 'State_zhvi.csv')
ZORI_FILE = os.path.join('data', 'State_zori.csv')

if __name__ == "__main__":
    print("Starting Zillow raw data load...")

    # Load ZHVI CSV
    df_zhvi = pd.read_csv(ZHVI_FILE)
    zhvi_rows = load_zhvi(df_zhvi)

    # Load ZORI CSV
    df_zori = pd.read_csv(ZORI_FILE)
    zori_rows = load_zori(df_zori)

    print(f"✓ Finished loading raw data: {zhvi_rows} ZHVI rows, {zori_rows} ZORI rows")
