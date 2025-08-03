#!/usr/bin/env python3
"""Quick exploratory statistics for RNS data.

Usage:
    python scripts/eda_rns.py [--samples N]

Loads all downloaded RNS JSONL files from ``data/raw/rns`` and
prints basic statistics, date distributions, and content analysis.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import matplotlib.pyplot as plt
import pandas as pd

RAW_DIR = Path("data/raw/rns")


def load_all_jsonl(raw_dir: Path) -> List[Dict]:
    """Load all JSONL files from raw_dir into a list of records."""
    records = []
    
    if not raw_dir.exists():
        return records
    
    jsonl_files = list(raw_dir.glob("*.jsonl"))
    
    if not jsonl_files:
        return records
    
    print(f"Found {len(jsonl_files)} JSONL files:")
    
    for jsonl_path in sorted(jsonl_files):
        print(f"  Loading {jsonl_path.name}")
        file_records = 0
        
        try:
            with open(jsonl_path, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue
                    
                    try:
                        record = json.loads(line)
                        records.append(record)
                        file_records += 1
                    except json.JSONDecodeError as e:
                        print(f"    Warning: Invalid JSON on line {line_num}: {e}")
                        
        except Exception as e:
            print(f"    Error reading {jsonl_path}: {e}")
            continue
            
        print(f"    Loaded {file_records} records")
    
    return records


def analyze_dates(records: List[Dict]) -> None:
    """Analyze publication dates in the records."""
    print("\n" + "="*50)
    print("DATE ANALYSIS")
    print("="*50)
    
    dates = []
    date_formats = []
    
    for record in records:
        pub_date = record.get('published', '')
        if not pub_date:
            continue
            
        # Try to parse different date formats
        parsed_date = None
        date_format = None
        
        # Common RSS date formats
        for fmt in [
            "%a, %d %b %Y %H:%M:%S %Z",      # RFC 822
            "%a, %d %b %Y %H:%M:%S GMT", 
            "%Y-%m-%dT%H:%M:%S%z",           # ISO 8601
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
        ]:
            try:
                parsed_date = datetime.strptime(pub_date, fmt)
                date_format = fmt
                break
            except ValueError:
                continue
        
        if parsed_date:
            dates.append(parsed_date)
            date_formats.append(date_format)
    
    if dates:
        df_dates = pd.Series(dates)
        print(f"Successfully parsed {len(dates)}/{len(records)} dates")
        print(f"Date range: {df_dates.min()} to {df_dates.max()}")
        print(f"Time span: {(df_dates.max() - df_dates.min()).days} days")
        
        # Date format breakdown
        format_counts = Counter(date_formats)
        print(f"\nDate formats found:")
        for fmt, count in format_counts.most_common():
            print(f"  {fmt}: {count} records")
        
        # Plot date distribution
        plt.figure(figsize=(12, 6))
        
        plt.subplot(1, 2, 1)
        df_dates.dt.date.value_counts().sort_index().plot()
        plt.title("Publications by Date")
        plt.xlabel("Date")
        plt.ylabel("Count")
        plt.xticks(rotation=45)
        
        plt.subplot(1, 2, 2)
        df_dates.dt.hour.value_counts().sort_index().plot(kind='bar')
        plt.title("Publications by Hour")
        plt.xlabel("Hour of Day")
        plt.ylabel("Count")
        
        plt.tight_layout()
        plt.show()
    else:
        print("No valid dates found in records")


def analyze_content(records: List[Dict], show_samples: int = 5) -> None:
    """Analyze content fields in the records."""
    print("\n" + "="*50)
    print("CONTENT ANALYSIS")
    print("="*50)
    
    # Field statistics
    fields = ['title', 'summary', 'category', 'author', 'source']
    field_stats = {}
    
    for field in fields:
        values = [record.get(field, '') for record in records]
        non_empty = [v for v in values if v and str(v).strip()]
        
        field_stats[field] = {
            'total': len(values),
            'non_empty': len(non_empty),
            'fill_rate': len(non_empty) / len(values) if values else 0,
            'avg_length': sum(len(str(v)) for v in non_empty) / len(non_empty) if non_empty else 0,
            'values': non_empty
        }
    
    # Print field statistics
    print(f"{'Field':<12} {'Fill Rate':<10} {'Avg Length':<12} {'Sample Values'}")
    print("-" * 80)
    
    for field, stats in field_stats.items():
        fill_pct = f"{stats['fill_rate']:.1%}"
        avg_len = f"{stats['avg_length']:.1f}"
        sample = stats['values'][0] if stats['values'] else 'N/A'
        sample = (sample[:40] + '...') if len(str(sample)) > 40 else sample
        
        print(f"{field:<12} {fill_pct:<10} {avg_len:<12} {sample}")
    
    # Content length distributions
    if field_stats['title']['values'] and field_stats['summary']['values']:
        plt.figure(figsize=(12, 4))
        
        plt.subplot(1, 3, 1)
        title_lengths = [len(str(t)) for t in field_stats['title']['values']]
        plt.hist(title_lengths, bins=20, alpha=0.7)
        plt.title("Title Length Distribution")
        plt.xlabel("Characters")
        plt.ylabel("Frequency")
        
        plt.subplot(1, 3, 2)
        summary_lengths = [len(str(s)) for s in field_stats['summary']['values']]
        plt.hist(summary_lengths, bins=20, alpha=0.7)
        plt.title("Summary Length Distribution")
        plt.xlabel("Characters")
        plt.ylabel("Frequency")
        
        plt.subplot(1, 3, 3)
        # Category breakdown
        categories = [record.get('category', 'Unknown') for record in records]
        cat_counts = Counter(categories)
        top_cats = dict(cat_counts.most_common(10))
        
        plt.bar(range(len(top_cats)), list(top_cats.values()))
        plt.title("Top Categories")
        plt.xlabel("Category")
        plt.ylabel("Count")
        plt.xticks(range(len(top_cats)), list(top_cats.keys()), rotation=45, ha='right')
        
        plt.tight_layout()
        plt.show()
    
    # Show sample records
    if show_samples > 0 and records:
        print(f"\n" + "="*50)
        print(f"SAMPLE RECORDS (showing {min(show_samples, len(records))})")
        print("="*50)
        
        for i, record in enumerate(records[:show_samples]):
            print(f"\n--- Record {i+1} ---")
            print(f"Title: {record.get('title', 'N/A')}")
            print(f"Category: {record.get('category', 'N/A')}")
            print(f"Published: {record.get('published', 'N/A')}")
            print(f"Source: {record.get('source', 'N/A')}")
            
            summary = record.get('summary', '')
            if summary:
                summary_preview = (summary[:200] + '...') if len(summary) > 200 else summary
                print(f"Summary: {summary_preview}")
            
            if record.get('link'):
                print(f"Link: {record['link']}")


def analyze_sources(records: List[Dict]) -> None:
    """Analyze data sources and their characteristics."""
    print("\n" + "="*50)
    print("SOURCE BREAKDOWN")
    print("="*50)
    
    # Source statistics
    sources = [record.get('source', 'unknown') for record in records]
    source_counts = Counter(sources)
    
    print(f"Records by source:")
    total = len(records)
    for source, count in source_counts.most_common():
        pct = count / total * 100
        print(f"  {source:<15} {count:>6} ({pct:5.1f}%)")
    
    # Author/feed analysis
    authors = [record.get('author', 'unknown') for record in records]
    author_counts = Counter(authors)
    
    print(f"\nTop authors/feeds:")
    for author, count in author_counts.most_common(10):
        print(f"  {author:<30} {count:>4}")
    
    # Link domain analysis
    domains = []
    for record in records:
        link = record.get('link', '')
        if link and 'http' in link:
            try:
                # Extract domain
                domain = link.split('//')[1].split('/')[0]
                domains.append(domain)
            except:
                continue
    
    if domains:
        domain_counts = Counter(domains)
        print(f"\nTop link domains:")
        for domain, count in domain_counts.most_common(10):
            print(f"  {domain:<30} {count:>4}")


def main() -> None:
    parser = argparse.ArgumentParser(description="RNS data exploratory analysis")
    parser.add_argument(
        "--samples", 
        type=int, 
        default=5, 
        help="Number of sample records to display"
    )
    args = parser.parse_args()

    print("RNS DATA EXPLORATORY ANALYSIS")
    print("=" * 50)
    
    records = load_all_jsonl(RAW_DIR)
    
    if not records:
        print(f"\nNo data files found in {RAW_DIR}")
        print("Run one of these commands first:")
        print("  python scripts/download_rns_robust.py")
        print("  python scripts/download_rns_minimal.py")
        return
    
    print(f"\nLoaded {len(records)} total records")
    
    # Run all analyses
    analyze_dates(records)
    analyze_content(records, args.samples)
    analyze_sources(records)
    
    print(f"\n" + "="*50)
    print("SUMMARY")
    print("="*50)
    print(f"Total records: {len(records):,}")
    
    # Data quality assessment
    complete_records = 0
    for record in records:
        if (record.get('title') and 
            record.get('published') and 
            record.get('link')):
            complete_records += 1
    
    completeness = complete_records / len(records) if records else 0
    print(f"Complete records: {complete_records:,} ({completeness:.1%})")
    
    # Categorization
    categories = [record.get('category', '') for record in records]
    non_empty_cats = [c for c in categories if c and c.strip()]
    categorization_rate = len(non_empty_cats) / len(records) if records else 0
    print(f"Categorization rate: {categorization_rate:.1%}")
    
    print(f"\nData appears suitable for further processing: {'✓' if completeness > 0.8 else '⚠️'}")


if __name__ == "__main__":
    main()