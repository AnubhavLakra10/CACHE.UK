#!/usr/bin/env python3
"""Download latest Regulatory News Service (RNS) items with multiple methods.

Usage:
    python scripts/download_rns_robust.py [--limit N] [--method METHOD]

Uses multiple approaches: RSS feeds, web scraping, and API calls.
Stores items as JSON lines under ``data/raw/rns``.
"""

from __future__ import annotations

import argparse
import json
import re
import time
from datetime import datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen

import feedparser

# RSS sources with better error handling
RSS_SOURCES = [
    "https://www.investegate.co.uk/rss.aspx",
    "https://www.investegate.co.uk/rss.aspx?category=General",
    "https://www.investegate.co.uk/rss.aspx?category=Results",
    "https://www.hl.co.uk/shares/stock-market-summary/ftse-100/rss",
]

# Web scraping targets as fallback
WEB_SOURCES = [
    {
        "name": "LSE Market News",
        "url": "https://www.londonstockexchange.com/news/market-news",
        "selector_title": ".news-item-title",
        "selector_link": ".news-item-link",
        "selector_date": ".news-item-date"
    },
    {
        "name": "Investegate Recent",
        "url": "https://www.investegate.co.uk/",
        "selector_title": "h3 a",
        "selector_link": "h3 a", 
        "selector_date": ".date"
    }
]


def clean_xml_content(content: str) -> str:
    """Clean XML content to handle encoding issues."""
    # Remove null bytes and other problematic characters
    content = content.replace('\x00', '')
    
    # Fix common encoding issues
    content = content.replace('&amp;amp;', '&amp;')
    content = content.replace('&lt;', '<')
    content = content.replace('&gt;', '>')
    
    # Remove or escape problematic characters
    content = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', content)
    
    return content


def fetch_rss_robust(url: str, limit: int | None = None) -> list[dict]:
    """Fetch RSS with robust error handling."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'application/rss+xml, application/xml, text/xml, */*',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'en-GB,en;q=0.9',
        'Cache-Control': 'no-cache'
    }
    
    try:
        print(f"Trying RSS: {url}")
        
        # First try: Direct feedparser
        parsed = feedparser.parse(url, request_headers=headers)
        
        if parsed.entries:
            print(f"✓ Found {len(parsed.entries)} entries via feedparser")
            return process_rss_entries(parsed.entries, limit)
        
        # Second try: Manual request with cleaning
        req = Request(url, headers=headers)
        with urlopen(req, timeout=30) as response:
            content = response.read()
            
            # Try different encodings
            for encoding in ['utf-8', 'iso-8859-1', 'windows-1252']:
                try:
                    text_content = content.decode(encoding)
                    cleaned_content = clean_xml_content(text_content)
                    
                    # Parse cleaned content
                    parsed = feedparser.parse(cleaned_content)
                    
                    if parsed.entries:
                        print(f"✓ Found {len(parsed.entries)} entries via manual cleaning ({encoding})")
                        return process_rss_entries(parsed.entries, limit)
                        
                except UnicodeDecodeError:
                    continue
                    
        print(f"✗ No entries found in RSS feed")
        return []
        
    except Exception as e:
        print(f"✗ RSS failed: {e}")
        return []


def process_rss_entries(entries, limit: int | None = None) -> list[dict]:
    """Process RSS entries into standardized format."""
    if limit:
        entries = entries[:limit]
    
    records = []
    for entry in entries:
        record = {
            "title": entry.get("title", "").strip(),
            "link": entry.get("link", "").strip(),
            "published": entry.get("published", "").strip(),
            "summary": entry.get("summary", "").strip(),
            "category": entry.get("category", "").strip(),
            "author": entry.get("author", "").strip(),
            "guid": entry.get("guid", "").strip(),
            "source": "rss"
        }
        
        # Clean up summary (remove HTML tags)
        if record["summary"]:
            record["summary"] = re.sub(r'<[^>]+>', '', record["summary"]).strip()
        
        records.append(record)
    
    return records


def scrape_web_source(source_config: dict, limit: int | None = None) -> list[dict]:
    """Scrape news from web source as fallback."""
    try:
        print(f"Trying web scraping: {source_config['name']}")
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        req = Request(source_config['url'], headers=headers)
        with urlopen(req, timeout=30) as response:
            content = response.read().decode('utf-8', errors='ignore')
        
        # Simple regex-based extraction (could be enhanced with BeautifulSoup)
        titles = re.findall(r'<[^>]*class="[^"]*news[^"]*title[^"]*"[^>]*>([^<]+)', content, re.IGNORECASE)
        links = re.findall(r'href="([^"]*news[^"]*)"', content)
        
        records = []
        for i, title in enumerate(titles[:limit] if limit else titles):
            link = links[i] if i < len(links) else ""
            
            # Make relative URLs absolute
            if link and not link.startswith('http'):
                base_url = f"{urlparse(source_config['url']).scheme}://{urlparse(source_config['url']).netloc}"
                link = urljoin(base_url, link)
            
            record = {
                "title": title.strip(),
                "link": link,
                "published": datetime.now().isoformat(),
                "summary": "",
                "category": "",
                "author": source_config['name'],
                "guid": link,
                "source": "web_scraping"
            }
            records.append(record)
        
        print(f"✓ Scraped {len(records)} items from {source_config['name']}")
        return records
        
    except Exception as e:
        print(f"✗ Web scraping failed for {source_config['name']}: {e}")
        return []


def try_alternative_feeds() -> list[dict]:
    """Try alternative RSS feeds that might work."""
    alternative_feeds = [
        "https://feeds.finance.yahoo.com/rss/2.0/headline?s=^FTSE&region=US&lang=en-US",
        "https://www.hl.co.uk/shares/stock-market-summary/ftse-100/rss",
        "https://www.sharecast.com/rss/news",
        "https://www.proactiveinvestors.co.uk/rss/news.xml"
    ]
    
    all_records = []
    
    for feed_url in alternative_feeds:
        try:
            records = fetch_rss_robust(feed_url, 10)
            if records:
                all_records.extend(records)
                time.sleep(1)  # Be nice to servers
        except Exception as e:
            print(f"Alternative feed failed: {e}")
            continue
    
    return all_records


def save_records(records: list[dict], out_dir: Path, method: str = "mixed") -> Path:
    """Save records to JSONL file."""
    out_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"rns_{method}_{timestamp}.jsonl"
    
    with open(out_path, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Download RNS items with multiple methods")
    parser.add_argument(
        "--method", 
        choices=["rss", "scraping", "alternative", "all"],
        default="all",
        help="Which method to use"
    )
    parser.add_argument(
        "--limit", type=int, default=20, help="Maximum number of items to fetch"
    )
    parser.add_argument(
        "--out-dir", default="data/raw/rns", help="Output directory"
    )
    args = parser.parse_args()

    all_records = []
    
    if args.method in ["rss", "all"]:
        print("🔄 Trying RSS feeds...")
        for rss_url in RSS_SOURCES:
            records = fetch_rss_robust(rss_url, args.limit)
            all_records.extend(records)
            time.sleep(1)
    
    if args.method in ["alternative", "all"] and not all_records:
        print("🔄 Trying alternative feeds...")
        alt_records = try_alternative_feeds()
        all_records.extend(alt_records)
    
    if args.method in ["scraping", "all"] and not all_records:
        print("🔄 Trying web scraping...")
        for source in WEB_SOURCES:
            records = scrape_web_source(source, args.limit)
            all_records.extend(records)
            time.sleep(2)
    
    if not all_records:
        print("❌ No data could be retrieved from any source")
        print("\nSuggestions:")
        print("1. Check your internet connection")
        print("2. Try again later (servers might be temporarily down)")
        print("3. Use --method scraping for basic web scraping")
        return
    
    # Remove duplicates based on title
    seen_titles = set()
    unique_records = []
    for record in all_records:
        if record["title"] not in seen_titles:
            seen_titles.add(record["title"])
            unique_records.append(record)
    
    if unique_records:
        out_path = save_records(unique_records, Path(args.out_dir), args.method)
        print(f"✅ Saved {len(unique_records)} unique items to {out_path}")
        
        # Show sample
        print(f"\nSample items:")
        for i, record in enumerate(unique_records[:3]):
            print(f"{i+1}. {record['title'][:80]}...")
    else:
        print("❌ No unique records found")


if __name__ == "__main__":
    main()