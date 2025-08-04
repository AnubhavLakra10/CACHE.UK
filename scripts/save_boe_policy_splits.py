#!/usr/bin/env python3
import os

from datasets import concatenate_datasets, load_dataset


def main():
    # The three 1K-row configs on HF
    configs = ["5768", "78516", "944601"]
    output_base = "data/raw/boe_policy_full"
    os.makedirs(output_base, exist_ok=True)

    # Collect datasets per split
    all_splits = {}
    for cfg in configs:
        print(f"Downloading config {cfg}...")
        ds = load_dataset("gtfintechlab/bank_of_england", cfg)
        for split_name, subset in ds.items():
            all_splits.setdefault(split_name, []).append(subset)

    # Concatenate and save each split
    for split_name, subsets in all_splits.items():
        print(f"Concatenating {split_name} ({len(subsets)} parts)...")
        concatenated = concatenate_datasets(subsets)
        outdir = os.path.join(output_base, split_name)
        concatenated.save_to_disk(outdir)
        print(f"✔ Saved full {split_name} → {outdir} ({len(concatenated)} rows)")

if __name__ == "__main__":
    main()
