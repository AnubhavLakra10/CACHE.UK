#!/usr/bin/env python3
"""Clean Companies House data and prepare Hugging Face dataset shards.

Usage:
    python scripts/prepare_hf_dataset.py [--input-dir DATA/raw/ch --out-dir data/cleaned/ch]

The script loads all BasicCompanyData zip files, filters out records with
impossible incorporation dates and extreme company-name lengths, adds metadata
fields, and saves the cleaned data as Hugging Face datasets sharded to a
manageable size.
"""
from __future__ import annotations

import argparse
from pathlib import Path
import zipfile

import pandas as pd
from datasets import Dataset

RAW_DIR = Path("data/raw/ch")
OUT_DIR = Path("data/cleaned/ch")


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


def clean_dataframe(
    df: pd.DataFrame, *, max_name_len: int = 80, min_date: str = "1900-01-01"
) -> pd.DataFrame:
    """Filter dates and company names, add metadata fields."""
    df = df.copy()

    if "IncorporationDate" in df.columns:
        dates = pd.to_datetime(df["IncorporationDate"], errors="coerce")
        today = pd.Timestamp.today().normalize()
        mask = (dates >= pd.Timestamp(min_date)) & (dates <= today)
        df = df[mask]
        df["date"] = dates[mask].dt.strftime("%Y-%m-%d")
    else:
        df["date"] = pd.NA

    if "CompanyName" in df.columns:
        names = df["CompanyName"].astype(str)
        mask = names.str.len().between(1, max_name_len)
        df = df[mask]
        df["CompanyName"] = names[mask]

    df["source"] = "ch"
    return df


def shard_and_save(df: pd.DataFrame, out_dir: Path, shard_size: int) -> None:
    """Save DataFrame as sharded Hugging Face datasets."""
    dataset = Dataset.from_pandas(df, preserve_index=False)
    out_dir.mkdir(parents=True, exist_ok=True)
    total = len(dataset)
    for start in range(0, total, shard_size):
        end = min(start + shard_size, total)
        shard = dataset.select(range(start, end))
        shard_dir = out_dir / f"part_{start // shard_size:05d}"
        shard.save_to_disk(shard_dir)
        print(f"Saved rows {start}-{end - 1} to {shard_dir}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Clean Companies House data and prepare dataset shards"
    )
    parser.add_argument("--input-dir", type=Path, default=RAW_DIR)
    parser.add_argument("--out-dir", type=Path, default=OUT_DIR)
    parser.add_argument("--shard-size", type=int, default=200_000)
    parser.add_argument("--max-name-len", type=int, default=80)
    parser.add_argument("--min-date", type=str, default="1900-01-01")
    args = parser.parse_args()

    if not args.input_dir.exists():
        print(f"No data directory found at {args.input_dir}")
        return

    df = load_all_parts(args.input_dir)
    if df.empty:
        print("No data files present - run scripts/download_ch.py first")
        return

    df = clean_dataframe(
        df, max_name_len=args.max_name_len, min_date=args.min_date
    )
    shard_and_save(df, args.out_dir, args.shard_size)


if __name__ == "__main__":
    main()
