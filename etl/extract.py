"""
Zillow Data Extraction Module

This module handles the extraction of housing data from Zillow Research.
Data sources: https://www.zillow.com/research/data/

Available datasets:
- ZHVI (Zillow Home Value Index): Typical home values
- ZORI (Zillow Observed Rent Index): Typical rental prices
"""

import pandas as pd
from typing import Tuple


# Zillow Research direct download URLs for state-level data
ZHVI_URL = "https://files.zillowstatic.com/research/public_csvs/zhvi/State_zhvi_bdrmcnt_1_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"
ZORI_URL = "https://files.zillowstatic.com/research/public_csvs/zori/State_zori_sm_month.csv"


def extract_zhvi() -> pd.DataFrame:
    """
    Download ZHVI (Zillow Home Value Index) data from Zillow Research.

    Returns:
        pd.DataFrame: Wide-format DataFrame with monthly home values as columns
    """
    print(f"Downloading ZHVI data from {ZHVI_URL}")
    df = pd.read_csv(ZHVI_URL)
    print(f"✓ Downloaded ZHVI data: {len(df)} rows, {len(df.columns)} columns")
    return df


def extract_zori() -> pd.DataFrame:
    """
    Download ZORI (Zillow Observed Rent Index) data from Zillow Research.

    Returns:
        pd.DataFrame: Wide-format DataFrame with monthly rent values as columns
    """
    print(f"Downloading ZORI data from {ZORI_URL}")
    df = pd.read_csv(ZORI_URL)
    print(f"✓ Downloaded ZORI data: {len(df)} rows, {len(df.columns)} columns")
    return df


def extract_all() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Download both ZHVI and ZORI datasets.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: (zhvi_df, zori_df)
    """
    zhvi_df = extract_zhvi()
    zori_df = extract_zori()
    return zhvi_df, zori_df
