#!/usr/bin/env python3
"""Download Companies House current snapshot files.

Usage:
    python scripts/download_ch.py

Downloads the current BasicCompanyData snapshot (all parts).
Files are stored under ``data/raw/ch``.
"""

from __future__ import annotations

import re
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

BASE_URL = "https://download.companieshouse.gov.uk"


def get_current_snapshot_info():
    """Scrape the download page to find current snapshot files."""
    url = f"{BASE_URL}/en_output.html"
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})

    try:
        with urlopen(req) as resp:
            content = resp.read().decode("utf-8")

        # Find all BasicCompanyData file links
        pattern = r"BasicCompanyData-(\d{4}-\d{2}-\d{2})-part(\d+)_(\d+)\.zip"
        matches = re.findall(pattern, content)

        if not matches:
            print("No BasicCompanyData files found on download page")
            return None, []

        # Get the date and total parts from first match
        date_str = matches[0][0]
        total_parts = int(matches[0][2])

        # Generate all part filenames
        files = []
        for part in range(1, total_parts + 1):
            filename = f"BasicCompanyData-{date_str}-part{part}_{total_parts}.zip"
            files.append(filename)

        return date_str, files

    except Exception as e:
        print(f"Failed to get snapshot info: {e}")
        return None, []


def download_file(filename: str, out_dir: Path) -> bool:
    """Download a single file."""
    url = f"{BASE_URL}/{filename}"
    dest = out_dir / filename

    if dest.exists():
        print(f"Skipping {filename}, already exists")
        return True

    print(f"Fetching {url}")
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})

    try:
        with urlopen(req) as resp, open(dest, "wb") as f:
            f.write(resp.read())
        print(f"Downloaded {filename}")
        return True
    except HTTPError as e:
        print(f"Failed to download {filename}: HTTP {e.code}")
        return False
    except URLError as e:
        print(f"Failed to download {filename}: {e.reason}")
        return False


def main() -> None:
    out_dir = Path("data/raw/ch")
    out_dir.mkdir(parents=True, exist_ok=True)

    print("Getting current snapshot information...")
    date_str, files = get_current_snapshot_info()

    if not files:
        print("No files to download")
        return

    print(f"Found snapshot from {date_str} with {len(files)} parts")

    success_count = 0
    for filename in files:
        if download_file(filename, out_dir):
            success_count += 1

    print(f"\nDownloaded {success_count}/{len(files)} files successfully")

    if success_count == len(files):
        print(f"All files saved to: {out_dir}")
    else:
        print("Some downloads failed - check the output above")


if __name__ == "__main__":
    main()
