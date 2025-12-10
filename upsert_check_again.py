#!/usr/bin/env python3
"""
Upsert check_these_again_results.csv to Supabase temp_address_census_tracts table.
"""

import csv
import requests
import time
import json

SUPABASE_URL = "https://db.smb.co/rest/v1/temp_address_census_tracts"
API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVnc2pwcWNmamVsaHF5Z2FxcXRiIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MDc5NDg1MDcsImV4cCI6MjAyMzUyNDUwN30.0ZrVIkhMF0btp5rhdg591byj0Meb3rsKrfPJanXx2kM"

BATCH_SIZE = 1000  # Supabase typically handles batches well

def upsert_batch(records: list) -> dict:
    """Upsert a batch of records to Supabase."""
    headers = {
        "apikey": API_KEY,
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"
    }
    
    response = requests.post(SUPABASE_URL, headers=headers, json=records, timeout=60)
    
    return {
        "status_code": response.status_code,
        "success": response.status_code in [200, 201],
        "text": response.text[:200] if response.text else ""
    }


def main():
    print("=" * 60)
    print("Upserting check_these_again_results.csv to Supabase")
    print("=" * 60)
    print()
    
    # Load records
    records = []
    with open('output/check_these_again_results.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Convert to proper format for the table
            record = {
                "id": int(row['id']),
                "census_tract_geoid": row['census_tract_geoid'] if row['census_tract_geoid'] else None
            }
            records.append(record)
    
    print(f"Total records to upsert: {len(records):,}")
    print(f"Batch size: {BATCH_SIZE}")
    print(f"Total batches: {(len(records) + BATCH_SIZE - 1) // BATCH_SIZE}")
    print()
    
    # Process in batches
    success_count = 0
    error_count = 0
    start_time = time.time()
    
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        total_batches = (len(records) + BATCH_SIZE - 1) // BATCH_SIZE
        
        result = upsert_batch(batch)
        
        if result['success']:
            success_count += len(batch)
        else:
            error_count += len(batch)
            print(f"  ❌ Batch {batch_num} failed: {result['status_code']} - {result['text']}")
        
        # Progress update every 50 batches
        if batch_num % 50 == 0 or batch_num == total_batches:
            elapsed = time.time() - start_time
            rate = success_count / elapsed if elapsed > 0 else 0
            pct = (i + len(batch)) / len(records) * 100
            print(f"📊 Progress: {i + len(batch):,}/{len(records):,} ({pct:.1f}%) | {rate:.0f} rec/sec | Batch {batch_num}/{total_batches}")
        
        # Small delay to avoid rate limiting
        time.sleep(0.05)
    
    elapsed = time.time() - start_time
    
    print()
    print("=" * 60)
    print("COMPLETE")
    print("=" * 60)
    print(f"✅ Success: {success_count:,}")
    print(f"❌ Errors:  {error_count:,}")
    print(f"Time: {elapsed:.1f}s ({success_count/elapsed:.0f} rec/sec)")


if __name__ == "__main__":
    main()
