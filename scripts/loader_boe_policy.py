from datasets import load_dataset

def load_boe_policy(split: str = None):
    ds = load_dataset("gtfintechlab/bank_of_england", "5768")
    if split:
        key = "val" if split in ("val", "validation") else split
        return ds[key]
    return ds

if __name__ == "__main__":
    all_ds = load_boe_policy()
    print(all_ds)
    print(f"Train rows: {len(load_boe_policy('train'))}")
    print(f"Val rows:   {len(load_boe_policy('val'))}")
