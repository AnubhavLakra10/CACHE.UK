#!/usr/bin/env python3
"""Working RNS scraper using real, active sources.

Usage:
    python scripts/download_rns_working.py [--limit N] [--days-back N]

Uses proven working sources for UK RNS data:
- Investegate direct scraping (most comprehensive)
- LSE news pages (direct scraping)
- Alternative financial news sources with RNS content
"""

from __future__ import annotations

import argparse
import html
import json
import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen

# Working sources - focused on direct web scraping since RSS feeds are broken
WORKING_SOURCES = [
    {
        "name": "Investegate_Direct",
        "type": "scrape",
        "url": "https://www.investegate.co.uk/",
        "priority": 1,
        "description": "Main Investegate page - most comprehensive RNS source",
    },
    {
        "name": "Investegate_Search",
        "type": "scrape",
        "url": "https://www.investegate.co.uk/search.aspx?q=&sort=date",
        "priority": 1,
        "description": "Investegate search results sorted by date",
    },
    {
        "name": "LSE_News_Direct",
        "type": "scrape",
        "url": "https://www.londonstockexchange.com/news",
        "priority": 2,
        "description": "LSE news page direct scraping",
    },
    {
        "name": "ShareCast_UK",
        "type": "scrape",
        "url": "https://www.sharecast.com/market-news",
        "priority": 2,
        "description": "ShareCast UK market news",
    },
    {
        "name": "Morningstar_UK",
        "type": "scrape",
        "url": "https://www.morningstar.co.uk/uk/news/",
        "priority": 3,
        "description": "Morningstar UK news",
    },
    {
        "name": "Yahoo_Finance_UK",
        "type": "rss",
        "url": "https://feeds.finance.yahoo.com/rss/2.0/headline?s=^FTSE&region=GB&lang=en-GB",
        "priority": 3,
        "description": "Yahoo Finance UK RSS (still working)",
    },
    {
        "name": "Proactive_UK",
        "type": "scrape",
        "url": "https://www.proactiveinvestors.co.uk/news/",
        "priority": 3,
        "description": "ProActive Investors UK news",
    },
]


def clean_text(text: str) -> str:
    """Clean and normalize text content."""
    if not text:
        return ""

    # Decode HTML entities
    text = html.unescape(text)

    # Remove HTML tags
    text = re.sub(r"<[^>]+>", "", text)

    # Clean up whitespace
    text = re.sub(r"\s+", " ", text).strip()

    # Remove common artifacts
    text = text.replace("\\n", " ").replace("\\t", " ")

    return text


def extract_company_ticker(title: str, content: str = "") -> dict:
    """Extract company name and ticker from content."""
    company_info = {"ticker": "", "company_name": ""}

    combined_text = f"{title} {content}"

    # Pattern 1: Company Name (TICKER:LON) or (TICKER.L)
    ticker_pattern = r"([A-Z][^(]*?)\s*\(([A-Z]{2,5})(?:[:.](?:LON|L))?\)"
    match = re.search(ticker_pattern, title)
    if match:
        company_info["company_name"] = match.group(1).strip()
        company_info["ticker"] = match.group(2).strip()
        return company_info

    # Pattern 2: Just ticker in parentheses
    ticker_only = re.search(r"\(([A-Z]{2,5})\)", title)
    if ticker_only:
        company_info["ticker"] = ticker_only.group(1)
        # Try to extract company name before ticker
        company_match = re.search(r"^([^(]+)", title)
        if company_match:
            company_info["company_name"] = company_match.group(1).strip()

    # Pattern 3: Ticker at start like "LLOY: Lloyd's Banking Group announces..."
    start_ticker = re.search(r"^([A-Z]{2,5}):\s*(.+)", title)
    if start_ticker:
        company_info["ticker"] = start_ticker.group(1)
        rest_title = start_ticker.group(2)
        # Extract company name from the rest
        company_match = re.search(r"^([^:,]+)", rest_title)
        if company_match:
            company_info["company_name"] = company_match.group(1).strip()

    return company_info


def classify_announcement(title: str, content: str = "") -> str:
    """Classify the type of announcement."""
    text = f"{title} {content}".lower()

    # Classification keywords
    categories = {
        "results": [
            "results",
            "earnings",
            "interim",
            "final",
            "half year",
            "full year",
            "quarterly",
            "q1",
            "q2",
            "q3",
            "q4",
        ],
        "trading_update": ["trading update", "trading statement", "guidance", "outlook", "forecast"],
        "acquisition": ["acquisition", "merger", "takeover", "purchase", "buy", "acquire"],
        "disposal": ["disposal", "sale", "sell", "divestment", "divest"],
        "dividend": ["dividend", "distribution", "payment", "payout", "yield"],
        "appointment": ["appointment", "director", "ceo", "cfo", "chairman", "board", "joins", "named"],
        "fundraising": ["fundraising", "placing", "rights issue", "share issue", "capital", "funding"],
        "contract": ["contract", "award", "wins", "secured", "agreement", "deal"],
        "regulatory": ["regulatory", "compliance", "investigation", "fine", "penalty"],
        "other": [],
    }

    for category, keywords in categories.items():
        if category == "other":
            continue
        if any(keyword in text for keyword in keywords):
            return category

    return "other"


def scrape_investegate_main(limit: int = 50) -> list[dict]:
    """Scrape main Investegate page - most reliable RNS source."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.5",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
    }

    records = []

    try:
        print("🔍 Scraping Investegate main page...")

        # Try main page first
        req = Request("https://www.investegate.co.uk/", headers=headers)
        with urlopen(req, timeout=30) as response:
            content = response.read().decode("utf-8", errors="ignore")

        # Updated patterns for current Investegate structure
        patterns = [
            # Pattern 1: Standard news item structure
            r'<div[^>]*class="[^"]*news[^"]*"[^>]*>.*?<a[^>]*href="([^"]+)"[^>]*>([^<]+)</a>.*?<div[^>]*class="[^"]*date[^"]*"[^>]*>([^<]+)</div>',
            # Pattern 2: Alternative structure
            r'<h[23][^>]*><a[^>]*href="([^"]+)"[^>]*>([^<]+)</a></h[23]>.*?<span[^>]*class="[^"]*date[^"]*"[^>]*>([^<]+)</span>',
            # Pattern 3: Simple link pattern
            r'href="(/news/[^"]+)"[^>]*>([^<]+RNS[^<]*)</a>',
            # Pattern 4: General announcement pattern
            r'<a[^>]*href="(/[^"]+)"[^>]*title="([^"]*)"[^>]*>([^<]+)</a>',
        ]

        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE | re.DOTALL)

            for match in matches[:limit]:
                if len(match) >= 2:
                    link = match[0]
                    title = clean_text(match[1])
                    date_str = match[2] if len(match) > 2 else ""

                    # Skip if title is too short or generic
                    if len(title) < 10 or title.lower() in ["more", "read more", "click here"]:
                        continue

                    # Make relative URLs absolute
                    if link.startswith("/"):
                        link = "https://www.investegate.co.uk" + link
                    elif not link.startswith("http"):
                        link = "https://www.investegate.co.uk/" + link

                    # Extract company info
                    company_info = extract_company_ticker(title)
                    announcement_type = classify_announcement(title)

                    record = {
                        "title": title,
                        "link": link,
                        "published": date_str or datetime.now().isoformat(),
                        "summary": "",  # We can fetch this later if needed
                        "category": "rns",
                        "author": "Investegate",
                        "guid": link,
                        "source": "Investegate_Direct",
                        "source_url": "https://www.investegate.co.uk/",
                        "source_priority": 1,
                        "ticker": company_info["ticker"],
                        "company_name": company_info["company_name"],
                        "rns_type": announcement_type,
                        "scraped_at": datetime.utcnow().isoformat(),
                    }

                    records.append(record)

            if records:  # If we found records with this pattern, use them
                break

        # If no structured content found, try fallback approach
        if not records:
            print("   Trying fallback extraction...")

            # Look for any links that might be RNS-related
            all_links = re.findall(r'<a[^>]*href="([^"]+)"[^>]*>([^<]+)</a>', content)

            for link, title in all_links[: limit * 2]:  # Check more links
                title = clean_text(title)

                # Filter for RNS-like content
                rns_indicators = ["rns", "announces", "results", "update", "dividend", "acquisition", "appoint"]
                if any(indicator in title.lower() for indicator in rns_indicators) and len(title) > 15:

                    if link.startswith("/"):
                        link = "https://www.investegate.co.uk" + link

                    company_info = extract_company_ticker(title)
                    announcement_type = classify_announcement(title)

                    record = {
                        "title": title,
                        "link": link,
                        "published": datetime.now().isoformat(),
                        "summary": "",
                        "category": "rns",
                        "author": "Investegate",
                        "guid": link,
                        "source": "Investegate_Direct",
                        "source_url": "https://www.investegate.co.uk/",
                        "source_priority": 1,
                        "ticker": company_info["ticker"],
                        "company_name": company_info["company_name"],
                        "rns_type": announcement_type,
                        "scraped_at": datetime.utcnow().isoformat(),
                    }

                    records.append(record)

                    if len(records) >= limit:
                        break

        print(f"   ✅ Found {len(records)} records from Investegate")
        return records

    except Exception as e:
        print(f"   ❌ Investegate scraping failed: {e}")
        return []


def scrape_lse_news(limit: int = 30) -> list[dict]:
    """Scrape LSE news page."""
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

    records = []

    try:
        print("🔍 Scraping LSE news...")

        req = Request("https://www.londonstockexchange.com/news", headers=headers)
        with urlopen(req, timeout=30) as response:
            content = response.read().decode("utf-8", errors="ignore")

        # Look for news items - LSE uses various structures
        patterns = [
            r'<article[^>]*>.*?<h[23][^>]*>.*?<a[^>]*href="([^"]+)"[^>]*>([^<]+)</a>',
            r'<div[^>]*news[^>]*>.*?<a[^>]*href="([^"]+)"[^>]*>([^<]+)</a>',
            r'"url":"([^"]+)"[^}]*"title":"([^"]+)"',
        ]

        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE | re.DOTALL)

            for match in matches[:limit]:
                link, title = match[0], clean_text(match[1])

                if len(title) < 10:
                    continue

                if not link.startswith("http"):
                    link = urljoin("https://www.londonstockexchange.com/", link)

                company_info = extract_company_ticker(title)
                announcement_type = classify_announcement(title)

                record = {
                    "title": title,
                    "link": link,
                    "published": datetime.now().isoformat(),
                    "summary": "",
                    "category": "regulatory",
                    "author": "LSE",
                    "guid": link,
                    "source": "LSE_News_Direct",
                    "source_url": "https://www.londonstockexchange.com/news",
                    "source_priority": 2,
                    "ticker": company_info["ticker"],
                    "company_name": company_info["company_name"],
                    "rns_type": announcement_type,
                    "scraped_at": datetime.utcnow().isoformat(),
                }

                records.append(record)

            if records:
                break

        print(f"   ✅ Found {len(records)} records from LSE")
        return records

    except Exception as e:
        print(f"   ❌ LSE scraping failed: {e}")
        return []


def fetch_yahoo_finance_rss(limit: int = 20) -> list[dict]:
    """Fetch from Yahoo Finance RSS - one of the few working RSS feeds."""
    import feedparser

    try:
        print("🔍 Fetching Yahoo Finance RSS...")

        url = "https://feeds.finance.yahoo.com/rss/2.0/headline?s=^FTSE&region=GB&lang=en-GB"

        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

        parsed = feedparser.parse(url, request_headers=headers)

        records = []
        for entry in parsed.entries[:limit]:
            title = clean_text(entry.get("title", ""))
            summary = clean_text(entry.get("summary", ""))

            company_info = extract_company_ticker(title, summary)
            announcement_type = classify_announcement(title, summary)

            record = {
                "title": title,
                "link": entry.get("link", ""),
                "published": entry.get("published", ""),
                "summary": summary,
                "category": entry.get("category", ""),
                "author": "Yahoo Finance",
                "guid": entry.get("guid", ""),
                "source": "Yahoo_Finance_UK",
                "source_url": url,
                "source_priority": 3,
                "ticker": company_info["ticker"],
                "company_name": company_info["company_name"],
                "rns_type": announcement_type,
                "scraped_at": datetime.utcnow().isoformat(),
            }

            records.append(record)

        print(f"   ✅ Found {len(records)} records from Yahoo Finance")
        return records

    except Exception as e:
        print(f"   ❌ Yahoo Finance RSS failed: {e}")
        return []


def scrape_sharecast(limit: int = 20) -> list[dict]:
    """Scrape ShareCast UK market news."""
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

    records = []

    try:
        print("🔍 Scraping ShareCast...")

        req = Request("https://www.sharecast.com/market-news", headers=headers)
        with urlopen(req, timeout=30) as response:
            content = response.read().decode('utf-8', errors='ignore')

        # ShareCast patterns
        patterns = [
            r'<h[23][^>]*><a[^>]*href="([^"]+)"[^>]*>([^<]+)</a></h[23]>',
            r'<a[^>]*href="([^"]+)"[^>]*class="[^"]*headline[^"]*"[^>]*>([^<]+)</a>',
        ]

        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)

            for match in matches[:limit]:
                link, title = match[0], clean_text(match[1])

                if len(title) < 10:
                    continue

                if not link.startswith("http"):
                    link = urljoin("https://www.sharecast.com/", link)

                company_info = extract_company_ticker(title)
                announcement_type = classify_announcement(title)

                record = {
                    "title": title,
                    "link": link,
                    "published": datetime.now().isoformat(),
                    "summary": "",
                    "category": "market_news",
                    "author": "ShareCast",
                    "guid": link,
                    "source": "ShareCast_UK",
                    "source_url": "https://www.sharecast.com/market-news",
                    "source_priority": 2,
                    "ticker": company_info["ticker"],
                    "company_name": company_info["company_name"],
                    "rns_type": announcement_type,
                    "scraped_at": datetime.utcnow().isoformat(),
                }

                records.append(record)

            if records:
                break

        print(f"   ✅ Found {len(records)} records from ShareCast")
        return records

    except Exception as e:
        print(f"   ❌ ShareCast scraping failed: {e}")
        return []


def remove_similar_duplicates(records: list[dict], similarity_threshold: float = 0.8) -> list[dict]:
    """Remove very similar records (not just exact duplicates)."""

    def similarity(s1: str, s2: str) -> float:
        """Simple similarity score based on common words."""
        if not s1 or not s2:
            return 0.0

        words1 = set(s1.lower().split())
        words2 = set(s2.lower().split())

        if len(words1) == 0 or len(words2) == 0:
            return 0.0

        intersection = len(words1.intersection(words2))
        union = len(words1.union(words2))

        return intersection / union if union > 0 else 0.0

    unique_records = []

    for record in records:
        title = record.get("title", "")

        # Check against existing records
        is_duplicate = False
        for existing in unique_records:
            existing_title = existing.get("title", "")

            if similarity(title, existing_title) > similarity_threshold:
                is_duplicate = True
                break

        if not is_duplicate:
            unique_records.append(record)

    return unique_records


def save_with_metadata(records: list[dict], out_dir: Path) -> Path:
    """Save records with comprehensive metadata."""
    out_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"rns_working_{timestamp}.jsonl"

    # Generate metadata
    metadata = {
        "total_records": len(records),
        "scraped_at": datetime.utcnow().isoformat(),
        "sources": {
            source: sum(1 for r in records if r.get("source") == source)
            for source in set(r.get("source", "") for r in records)
        },
        "rns_types": {
            rns_type: sum(1 for r in records if r.get("rns_type") == rns_type)
            for rns_type in set(r.get("rns_type", "") for r in records)
        },
        "companies_identified": sum(1 for r in records if r.get("company_name")),
        "tickers_identified": sum(1 for r in records if r.get("ticker")),
        "unique_companies": len(set(r.get("company_name", "") for r in records if r.get("company_name"))),
        "unique_tickers": len(set(r.get("ticker", "") for r in records if r.get("ticker"))),
    }

    with open(out_path, "w", encoding="utf-8") as f:
        # Write metadata as first line
        f.write(json.dumps({"_metadata": metadata}, ensure_ascii=False) + "\n")

        # Write records
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Download RNS data using working sources")
    parser.add_argument("--limit", type=int, default=50, help="Items per source")
    parser.add_argument("--out-dir", default="data/raw/rns", help="Output directory")
    parser.add_argument(
        "--sources",
        nargs="+",
        choices=["investegate", "lse", "yahoo", "sharecast", "all"],
        default=["all"],
        help="Which sources to use",
    )
    args = parser.parse_args()

    print("🚀 Starting RNS data collection with working sources...")

    all_records = []

    # Determine which sources to use
    use_sources = args.sources
    if "all" in use_sources:
        use_sources = ["investegate", "lse", "yahoo", "sharecast"]

    # Execute scrapers
    if "investegate" in use_sources:
        records = scrape_investegate_main(args.limit)
        all_records.extend(records)
        time.sleep(2)

    if "lse" in use_sources:
        records = scrape_lse_news(args.limit)
        all_records.extend(records)
        time.sleep(2)

    if "yahoo" in use_sources:
        records = fetch_yahoo_finance_rss(args.limit)
        all_records.extend(records)
        time.sleep(2)

    if "sharecast" in use_sources:
        records = scrape_sharecast(args.limit)
        all_records.extend(records)
        time.sleep(2)

    if not all_records:
        print("❌ No data retrieved from any source")
        print("\nTroubleshooting:")
        print("1. Check internet connection")
        print("2. Try individual sources: --sources investegate")
        print("3. Some sites may be temporarily down")
        return

    print(f"\n📊 Raw data collected: {len(all_records):,} records")

    # Remove duplicates and very similar records
    before_dedup = len(all_records)
    unique_records = remove_similar_duplicates(all_records, similarity_threshold=0.7)
    print(f"🔄 After deduplication: {len(unique_records):,} records (removed {before_dedup - len(unique_records)})")

    # Save data
    out_path = save_with_metadata(unique_records, Path(args.out_dir))
    print(f"✅ Saved {len(unique_records):,} records to {out_path}")

    # Show summary
    sources = {}
    rns_types = {}
    companies = {}

    for record in unique_records:
        source = record.get("source", "unknown")
        sources[source] = sources.get(source, 0) + 1

        rns_type = record.get("rns_type", "unknown")
        rns_types[rns_type] = rns_types.get(rns_type, 0) + 1

        company = record.get("company_name", "")
        if company:
            companies[company] = companies.get(company, 0) + 1

    print(f"\n📈 SUMMARY:")
    print(f"Sources: {dict(sorted(sources.items(), key=lambda x: x[1], reverse=True))}")
    print(f"RNS Types: {dict(sorted(rns_types.items(), key=lambda x: x[1], reverse=True))}")

    if companies:
        top_companies = dict(sorted(companies.items(), key=lambda x: x[1], reverse=True)[:10])
        print(f"Top Companies: {top_companies}")

    print(f"\n📋 Sample Records:")
    for i, record in enumerate(unique_records[:5]):
        ticker = f" ({record['ticker']})" if record.get("ticker") else ""
        print(f"{i+1}. [{record['rns_type']}] {record['title'][:80]}...{ticker}")

    print(f"\n✅ Collection complete! Run EDA with:")
    print(f"   python scripts/eda_rns.py --data-dir {args.out_dir}")


if __name__ == "__main__":
    main()
