#!/usr/bin/env python3
"""
Fetch missing records from gaps between address files.
Identifies gaps in ID sequence and recovers missing records.
"""

import requests
import csv
import time
import logging
import glob
import os
from typing import List, Dict, Any, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("fetch_gaps.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# API Configuration
BASE_URL = "https://egsjpqcfjelhqygaqqtb.supabase.co/rest/v1/addresses"
API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVnc2pwcWNmamVsaHF5Z2FxcXRiIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MDc5NDg1MDcsImV4cCI6MjAyMzUyNDUwN30.0ZrVIkhMF0btp5rhdg591byj0Meb3rsKrfPJanXx2kM"

LIMIT = 2000  # Records per request
MAX_RETRIES = 3
RETRY_DELAY = 2
OUTPUT_FILE = "addresses_gaps.csv"


def find_gaps() -> List[Tuple[int, int]]:
    """
    Analyze all part files and find gaps in ID sequence.
    Returns list of (start_id, end_id) tuples representing missing ranges.
    """
    pattern = "addresses_part_*.csv"
    files = sorted(glob.glob(pattern))
    
    if not files:
        logger.error("No address part files found!")
        return []
    
    gaps = []
    prev_last_id = None
    
    for filepath in files:
        filename = os.path.basename(filepath)
        
        # Get first and last ID from file
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            if not rows:
                continue
            
            first_id = int(rows[0]['id'])
            last_id = int(rows[-1]['id'])
        
        # Check for gap with previous file
        if prev_last_id is not None:
            expected_next = prev_last_id + 1
            if first_id > expected_next:
                gap_start = expected_next
                gap_end = first_id - 1
                gap_size = gap_end - gap_start + 1
                gaps.append((gap_start, gap_end))
                logger.info(f"Gap found: IDs {gap_start:,} to {gap_end:,} ({gap_size:,} potential records)")
        
        prev_last_id = last_id
    
    return gaps


def fetch_range(start_id: int, end_id: int) -> List[Dict[str, Any]]:
    """
    Fetch all records in an ID range using pagination.
    Uses ID-based cursor for reliable fetching.
    """
    all_records = []
    current_start = start_id
    
    while current_start <= end_id:
        headers = {"apiKey": API_KEY}
        params = {
            "select": "*",
            "and": f"(id.gte.{current_start},id.lte.{end_id})",
            "order": "id.asc",
            "limit": LIMIT
        }
        
        for attempt in range(MAX_RETRIES):
            try:
                response = requests.get(BASE_URL, headers=headers, params=params, timeout=60)
                response.raise_for_status()
                data = response.json()
                
                if not data:
                    return all_records
                
                all_records.extend(data)
                
                # Move cursor past last fetched ID
                last_fetched_id = data[-1]['id']
                current_start = last_fetched_id + 1
                
                if len(data) < LIMIT:
                    # No more records in range
                    return all_records
                
                break
                
            except requests.exceptions.RequestException as e:
                if attempt < MAX_RETRIES - 1:
                    logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying...")
                    time.sleep(RETRY_DELAY * (attempt + 1))
                else:
                    logger.error(f"All retries failed for range {start_id}-{end_id}: {e}")
                    raise
    
    return all_records


def main():
    logger.info("=" * 70)
    logger.info("Gap Recovery Script")
    logger.info("=" * 70)
    
    # Find all gaps
    logger.info("Scanning files for gaps...")
    gaps = find_gaps()
    
    if not gaps:
        logger.info("No gaps found! All records are continuous.")
        return
    
    total_gap_size = sum(end - start + 1 for start, end in gaps)
    logger.info(f"")
    logger.info(f"Found {len(gaps)} gaps with {total_gap_size:,} potential missing IDs")
    logger.info(f"")
    
    # Fetch missing records
    total_recovered = 0
    fieldnames = None
    
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = None
        
        for i, (start_id, end_id) in enumerate(gaps, 1):
            gap_size = end_id - start_id + 1
            logger.info(f"[{i}/{len(gaps)}] Fetching gap: IDs {start_id:,} to {end_id:,} ({gap_size:,} potential)")
            
            try:
                records = fetch_range(start_id, end_id)
                
                if records:
                    # Initialize writer with fieldnames from first batch
                    if writer is None:
                        fieldnames = list(records[0].keys())
                        writer = csv.DictWriter(f, fieldnames=fieldnames)
                        writer.writeheader()
                    
                    writer.writerows(records)
                    total_recovered += len(records)
                    logger.info(f"    Recovered {len(records):,} records")
                else:
                    logger.info(f"    No records found in this range (deleted IDs?)")
                    
            except Exception as e:
                logger.error(f"    Failed to fetch gap: {e}")
                continue
    
    logger.info("")
    logger.info("=" * 70)
    logger.info(f"✓ Gap recovery complete!")
    logger.info(f"✓ Total records recovered: {total_recovered:,}")
    logger.info(f"✓ Saved to: {OUTPUT_FILE}")
    logger.info("=" * 70)
    
    if total_recovered > 0:
        logger.info("")
        logger.info("Next steps:")
        logger.info("1. The recovered records are in addresses_gaps.csv")
        logger.info("2. You can merge all files or keep them separate")
        logger.info("3. To get a complete sorted dataset, combine all files")


if __name__ == "__main__":
    main()
