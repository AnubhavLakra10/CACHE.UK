import glob
import json
import os
from pathlib import Path

import pandas as pd

BASE = Path(__file__).parent.parent / "data" / "raw"
OVERVIEW = Path(__file__).parent.parent / "data_overview.md"

def folder_size(path: Path):
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())

def profile_folder(name, path):
    card = {"name": name, "path": str(path), "size_bytes": folder_size(path)}
    # find CSV/JSONL/SENT files
    files = list(path.rglob("*"))
    if any(str(f).endswith(".csv") for f in files):
        df = pd.concat([pd.read_csv(f, low_memory=False) for f in path.rglob("*.csv")], ignore_index=True)
        card["num_rows"] = len(df)
        # record all columns and if any are labels
        card["columns"] = df.columns.tolist()
        # if any known label cols, tally value counts
        for col in df.columns:
            if df[col].dtype == "object" or df[col].dtype == "category":
                vc = df[col].value_counts().to_dict()
                if len(vc) < 100 and len(vc) > 1:
                    card.setdefault("label_distributions", {})[col] = vc
    # JSONL
    elif any(str(f).endswith(".jsonl") for f in files):
        cnt = sum(1 for _ in path.rglob("*.jsonl") for _ in open(_))
        card["num_rows"] = cnt
        card["format"] = "jsonl"
    else:
        card["files"] = len(files)

    # ideal uses (manual mapping)
    mapping = {
        "ch":    "filings timeline & belief edits",
        "rns":   "announcement parsing & hallucination tests",
        "boe":   "macro time-series grounding",
        "px":    "price-based trend edits",
        "revisit_llm": "domain pretraining",
        "investing_strat": "signal backtest & RAG",
        "boe_cls": "stance classification & editing",
        "finnli":  "NLI reasoning & contradiction tests",
        "transcripts": "speech→text RAG & memory augment"
    }
    for k,v in mapping.items():
        if k in name:
            card["ideal_use"] = v

    # emit card
    out = path / "dataset_card.json"
    with open(out, "w") as f:
        json.dump(card, f, indent=2)
    return card

def main():
    md_lines = ["# Data Overview\n", "| Dataset | Rows | Size (MB) | Ideal Use |","|---|---|---|---|"]
    for sub in sorted(BASE.iterdir()):
        if not sub.is_dir(): continue
        card = profile_folder(sub.name, sub)
        num_rows = card.get('num_rows', '-')
        num_rows_fmt = f"{num_rows:,}" if isinstance(num_rows, int) else num_rows
        md_lines.append(f"| `{sub.name}` | {num_rows_fmt} | {card['size_bytes']/1e6:.1f} | {card.get('ideal_use','-')} |")

    with open(OVERVIEW, "w") as f:
        f.write("\n".join(md_lines))


if __name__=="__main__":
    main()
