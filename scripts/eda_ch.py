#!/usr/bin/env python3
"""Quick exploratory statistics for Companies House data.

Usage:
    python scripts/eda_ch.py

Loads all downloaded BasicCompanyData parts from ``data/raw/ch`` and
prints basic statistics. If the expected files are missing, a message is
shown and the script exits without error.
"""

from __future__ import annotations

from pathlib import Path
import zipfile

import pandas as pd
import matplotlib.pyplot as plt

RAW_DIR = Path("data/raw/ch")


def load_all_parts(raw_dir: Path) -> pd.DataFrame:
    """Load all zip files under ``raw_dir`` into a single DataFrame."""
    frames = []
    for zip_path in sorted(raw_dir.glob("*.zip")):
        with zipfile.ZipFile(zip_path) as zf:
            csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
            if not csv_names:
                continue
            with zf.open(csv_names[0]) as f:
                frames.append(pd.read_csv(f, dtype=str, low_memory=False))
    if frames:
        return pd.concat(frames, ignore_index=True)
    return pd.DataFrame()


def main() -> None:
    if not RAW_DIR.exists():
        print(f"No data directory found at {RAW_DIR}")
        return

    df = load_all_parts(RAW_DIR)
    if df.empty:
        print("No data files present - run scripts/download_ch.py first")
        return

    print(f"Total records: {len(df):,}")

    if "IncorporationDate" in df.columns:
        dates = pd.to_datetime(df["IncorporationDate"], errors="coerce")
        print(
            "Incorporation date range:",
            dates.min(),
            "to",
            dates.max(),
        )
        (dates.value_counts().sort_index()).plot()
        plt.title("Company incorporations over time")
        plt.xlabel("Date")
        plt.ylabel("Count")
        plt.tight_layout()
        plt.show()

    if "CompanyName" in df.columns:
        name_lengths = df["CompanyName"].astype(str).str.len()
        name_lengths.hist(bins=50)
        plt.title("Company name length distribution")
        plt.xlabel("Length")
        plt.ylabel("Frequency")
        plt.tight_layout()
        plt.show()


if __name__ == "__main__":
    main()
