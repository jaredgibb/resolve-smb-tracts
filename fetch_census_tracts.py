#!/usr/bin/env python3
"""
Fetch all census_tracts from Supabase API and save to CSV.
Handles pagination for the public.census_tracts table.
"""

import requests
import csv
import time
import logging
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from queue import Queue

# Configure logging
LOG_FILE = "fetch_census_tracts.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# API Configuration
BASE_URL = "https://egsjpqcfjelhqygaqqtb.supabase.co/rest/v1/census_tracts"
API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVnc2pwcWNmamVsaHF5Z2FxcXRiIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MDc5NDg1MDcsImV4cCI6MjAyMzUyNDUwN30.0ZrVIkhMF0btp5rhdg591byj0Meb3rsKrfPJanXx2kM"

# Pagination settings
LIMIT = 1000  # Records per request (slower as requested)
CONCURRENT_REQUESTS = 5  # 5 concurrent requests
OUTPUT_FILE = "census_tracts.csv"
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds base for exponential backoff
ROWS_PER_FILE = 500000  # Split into new file every 500k rows


def fetch_batch(offset: int, limit: int, retries: int = MAX_RETRIES) -> tuple[List[Dict[str, Any]], bool]:
    """
    Fetch a batch of records from the API with retry logic.
    
    Returns:
        tuple: (data list, has_more boolean)
    """
    headers = {
        "apiKey": API_KEY,
        "Range-Unit": "items"
    }
    
    params = {
        "select": "*",
        "offset": offset,
        "limit": limit,
        "order": "id.asc"
    }
    
    for attempt in range(retries):
        try:
            response = requests.get(BASE_URL, headers=headers, params=params, timeout=60)
            response.raise_for_status()
            
            data = response.json()
            has_more = len(data) == limit
            
            logger.debug(f"Fetched {len(data)} rows at offset {offset}")
            return data, has_more
            
        except requests.exceptions.RequestException as e:
            if attempt < retries - 1:
                logger.warning(
                    f"Attempt {attempt + 1} failed at offset {offset}: {e}. Retrying in {RETRY_DELAY * (attempt + 1)}s..."
                )
                time.sleep(RETRY_DELAY * (attempt + 1))
            else:
                logger.error(f"All {retries} attempts failed at offset {offset}: {e}")
                raise


def test_api_connection() -> bool:
    """Test if API is accessible and can handle batch size."""
    logger.info("Testing API connection...")
    try:
        headers = {"apiKey": API_KEY}
        params = {"select": "*", "limit": LIMIT, "offset": 0}
        response = requests.get(BASE_URL, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        logger.info(f"✓ API test successful! Retrieved {len(data)} records.")
        return True
        
    except Exception as e:
        logger.error(f"✗ API test failed: {e}")
        return False


def fetch_batch_concurrent(offset: int, limit: int, batch_id: int) -> tuple[int, List[Dict[str, Any]], bool]:
    """
    Fetch a batch of records from the API (for concurrent execution).
    Returns batch_id along with data for ordering.
    
    Returns:
        tuple: (batch_id, data list, has_more boolean)
    """
    try:
        data, has_more = fetch_batch(offset, limit)
        return batch_id, data, has_more
    except Exception as e:
        logger.error(f"Batch {batch_id} failed: {e}")
        raise


def get_output_filename(file_number: int) -> str:
    """Generate output filename with part number."""
    base, ext = OUTPUT_FILE.rsplit('.', 1) if '.' in OUTPUT_FILE else (OUTPUT_FILE, 'csv')
    return f"{base}_part_{file_number:03d}.{ext}"


def main():
    """Main function to fetch all census_tracts data and write to CSV."""
    
    logger.info("=" * 70)
    logger.info(f"Starting census_tracts fetch from Supabase API...")
    logger.info(f"Output file pattern: {OUTPUT_FILE.replace('.csv', '_part_XXX.csv')}")
    logger.info(f"Rows per file: {ROWS_PER_FILE:,}")
    logger.info(f"Batch size: {LIMIT} rows per request")
    logger.info(f"Concurrent requests: {CONCURRENT_REQUESTS}")
    logger.info("=" * 70)
    
    # Test API connection first
    if not test_api_connection():
        logger.error("Failed API test. Exiting.")
        return
    
    logger.info("")
    
    offset = 0
    total_rows = 0
    batch_number = 0
    csv_writer = None
    csv_file = None
    fieldnames = None
    start_time = time.time()
    csv_lock = threading.Lock()  # Lock for thread-safe CSV writing
    
    file_number = 1
    rows_in_current_file = 0
    
    try:
        # Open file
        current_filename = get_output_filename(file_number)
        csv_file = open(current_filename, 'w', newline='', encoding='utf-8')
        logger.info(f"Writing to: {current_filename}")
        logger.info("")
        
        while True:
            batch_start = time.time()
            
            with ThreadPoolExecutor(max_workers=CONCURRENT_REQUESTS) as executor:
                futures = {
                    executor.submit(
                        fetch_batch_concurrent,
                        offset + (i * LIMIT),
                        LIMIT,
                        batch_number + i,
                    ): i
                    for i in range(CONCURRENT_REQUESTS)
                }
                
                total_in_round = 0
                has_more_any = False
                
                for future in as_completed(futures):
                    batch_id, data, has_more = future.result()
                    current_offset = offset + (futures[future] * LIMIT)
                    
                    if not data:
                        logger.debug(f"Batch {batch_id} returned no data")
                        continue
                    
                    # Initialize fieldnames from first batch
                    if fieldnames is None:
                        fieldnames = list(data[0].keys())
                        logger.info(f"CSV headers: {', '.join(fieldnames)}")
                    
                    # Write rows to CSV (thread-safe)
                    with csv_lock:
                        rows_to_write = list(data)  # Batch of rows from this batch
                        
                        while rows_to_write:
                            # Check if we need to start a new file
                            space_in_file = ROWS_PER_FILE - rows_in_current_file
                            rows_for_this_file = rows_to_write[:space_in_file]
                            rows_to_write = rows_to_write[space_in_file:]
                            
                            if rows_in_current_file >= ROWS_PER_FILE:
                                csv_file.close()
                                file_number += 1
                                rows_in_current_file = 0
                                current_filename = get_output_filename(file_number)
                                csv_file = open(current_filename, 'w', newline='', encoding='utf-8')
                                csv_writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                                csv_writer.writeheader()
                                logger.info(f"")
                                logger.info(f"Starting new file: {current_filename}")
                                logger.info(f"")
                            
                            # Initialize CSV writer if needed (first file)
                            if csv_writer is None:
                                csv_writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                                csv_writer.writeheader()
                            
                            # Write batch of rows at once
                            if rows_for_this_file:
                                csv_writer.writerows(rows_for_this_file)
                                rows_in_current_file += len(rows_for_this_file)
                    
                    rows_fetched = len(data)
                    total_in_round += rows_fetched
                    total_rows += rows_fetched
                    has_more_any = has_more_any or has_more
                    
                    logger.info(
                        f"  Batch {batch_id} (offset {current_offset:,}): {rows_fetched} rows | Running total: {total_rows:,}"
                    )
            
            if csv_file:
                csv_file.flush()
            elapsed = time.time() - batch_start
            rows_per_sec = total_in_round / elapsed if elapsed > 0 else 0
            logger.info(
                f"Offset {offset:,}: {total_in_round} rows in {elapsed:.2f}s ({rows_per_sec:.0f} rows/sec) | Total: {total_rows:,}"
            )
            
            # Break if no more data
            if not has_more_any:
                logger.info("Reached end of data (fewer rows than limit).")
                break
            
            # Update offset for next round
            offset += LIMIT * CONCURRENT_REQUESTS
            batch_number += CONCURRENT_REQUESTS
            logger.info("")
        
        # Close final file
        if csv_file:
            csv_file.close()
        
        total_elapsed = time.time() - start_time
        logger.info("=" * 70)
        logger.info(f"✓ Fetch complete!")
        logger.info(f"✓ Total rows written: {total_rows:,}")
        logger.info(f"✓ Total files created: {file_number}")
        logger.info(f"✓ Total time: {total_elapsed:.2f}s ({total_elapsed/60:.1f} minutes)")
        logger.info(f"✓ Average rate: {total_rows/total_elapsed:.0f} rows/sec")
        logger.info("=" * 70)
        
    except KeyboardInterrupt:
        logger.warning(f"⚠ Interrupted by user!")
        logger.warning(f"Partial data saved: {total_rows:,} rows across {file_number} file(s)")
        if csv_file:
            csv_file.close()
        
    except Exception as e:
        logger.error(f"✗ Error: {e}", exc_info=True)
        logger.error(f"Partial data saved: {total_rows:,} rows")
        if csv_file:
            csv_file.close()
        raise


if __name__ == "__main__":
    main()
