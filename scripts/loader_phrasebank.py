"""
Loader for the Takala Financial Phrasebank dataset.

Place this file at: scripts/loader_phrasebank.py

Prerequisites:
    pip install datasets pandas

Usage:
    # 1. Download raw data into data/raw/phrasebank/
    python scripts/loader_phrasebank.py --config sentences_allagree

    # 2. Load in Python REPL
    python
    >>> from scripts.loader_phrasebank import load_phrasebank
    >>> df = load_phrasebank("sentences_allagree")
    >>> print(df.head())

    # Or as a one-liner from shell:
    python -c "from scripts.loader_phrasebank import load_phrasebank; df=load_phrasebank('sentences_allagree'); print(df.head())"

Functions:
    download_phrasebank(config):
        Fetches the specified split from Hugging Face and saves it as a CSV under data/raw/phrasebank/.

    load_phrasebank(config):
        Loads the saved CSV into a pandas DataFrame.

If imported in your code:
    from scripts.loader_phrasebank import download_phrasebank, load_phrasebank
    path = download_phrasebank("sentences_allagree")
    df = load_phrasebank("sentences_allagree")
"""

import argparse
import os

import pandas as pd
from datasets import get_dataset_config_names, load_dataset

# Directories and filename template
data_dir = os.path.join("data", "raw", "phrasebank")
filename_template = "financial_phrasebank_{config}.csv"

try:
    AVAILABLE_CONFIGS = get_dataset_config_names("takala/financial_phrasebank")
except Exception:
    AVAILABLE_CONFIGS = [
        "sentences_allagree",
        "sentences_75agree",
        "sentences_66agree",
        "sentences_50agree",
    ]


def download_phrasebank(config: str = "sentences_allagree") -> str:
    """
    Download the specified config of the Financial Phrasebank dataset
    and save it as a CSV under data/raw/phrasebank/.

    Returns the path to the saved CSV file.
    """
    if config not in AVAILABLE_CONFIGS:
        raise ValueError(f"Invalid config '{config}'. Choose from: {AVAILABLE_CONFIGS}")

    os.makedirs(data_dir, exist_ok=True)
    # Trust remote code to avoid warnings
    ds = load_dataset(
        "takala/financial_phrasebank", config,
        split="train", trust_remote_code=True
    )

    # Convert to pandas and add human-readable sentiment
    df = pd.DataFrame(ds)
    label_names = ds.features["label"].names
    df["sentiment"] = df["label"].map(lambda i: label_names[i])

    filepath = os.path.join(data_dir, filename_template.format(config=config))
    df.to_csv(filepath, index=False)
    print(f"Saved phrasebank ({config}) to {filepath}")
    return filepath


def load_phrasebank(config: str = "sentences_allagree") -> pd.DataFrame:
    """
    Load the previously downloaded CSV for the given config
    into a pandas DataFrame.
    """
    filepath = os.path.join(data_dir, filename_template.format(config=config))
    if not os.path.exists(filepath):
        raise FileNotFoundError(
            f"No data for config '{config}' found at {filepath}. Run download_phrasebank(config) first."
        )
    return pd.read_csv(filepath)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download or manage the Financial Phrasebank dataset."
    )
    parser.add_argument(
        "--config",
        type=str,
        default="sentences_allagree",
        choices=AVAILABLE_CONFIGS,
        help="Which agreement split to download",
    )
    args = parser.parse_args()
    download_phrasebank(args.config)
