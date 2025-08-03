#!/usr/bin/env python3
"""Download Companies House monthly ZIP files.

Usage:
    python scripts/download_ch.py --start 2024-01 --end 2024-03 --dataset Accounts_Bulk_Data

By default, downloads the current month's file for the BasicCompanyData feed.
Files are stored under ``data/raw/ch``.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

BASE_URL = "https://download.companieshouse.gov.uk"


def month_range(start: date, end: date):
    """Yield first-of-month dates from start to end inclusive."""
    current = start
    while current <= end:
        yield current
        # increment month
        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)


def download_file(prefix: str, month: date, out_dir: Path) -> None:
    """Download a single monthly file if possible."""
    filename = f"{prefix}-{month.year}-{month.month:02d}.zip"
    url = f"{BASE_URL}/{filename}"
    dest = out_dir / filename
    if dest.exists():
        print(f"Skipping {filename}, already exists")
        return

    print(f"Fetching {url}")
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urlopen(req) as resp, open(dest, "wb") as f:
            f.write(resp.read())
        print(f"Downloaded {filename}")
    except HTTPError as e:
        print(f"Failed to download {filename}: HTTP {e.code}")
    except URLError as e:
        print(f"Failed to download {filename}: {e.reason}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download Companies House monthly ZIPs")
    parser.add_argument("--start", type=str, help="Start date YYYY-MM", required=False)
    parser.add_argument("--end", type=str, help="End date YYYY-MM", required=False)
    parser.add_argument(
        "--dataset",
        type=str,
        default="basiccompanydata",
        help="Dataset prefix, e.g. accounts_bulk_data or BasicCompanyData (case-insensitive; underscores ignored)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    today = date.today().replace(day=1)
    start = (
        datetime.strptime(args.start, "%Y-%m").date() if args.start else today
    )
    end = datetime.strptime(args.end, "%Y-%m").date() if args.end else start

    out_dir = Path("data/raw/ch")
    out_dir.mkdir(parents=True, exist_ok=True)

    dataset = args.dataset.replace("_", "").lower()
    for month in month_range(start, end):
        download_file(dataset, month, out_dir)


if __name__ == "__main__":
    main()
