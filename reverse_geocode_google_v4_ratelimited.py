#!/usr/bin/env python3
"""
Rate-limited async reverse geocoding using Google Geocoding API v4 (FREE preview).
Uses token bucket algorithm to stay within ~25 QPS (1500/min, giving headroom for 1800/min limit).
"""

import csv
import asyncio
import aiohttp
import ssl
import certifi
import time
import os
from typing import Dict, List

# Google Geocoding API v4 (FREE during preview)
API_KEY = os.environ.get("GOOGLE_MAPS_API_KEY")
if not API_KEY:
    raise RuntimeError("GOOGLE_MAPS_API_KEY env var required for Google Geocoding API calls")
V4_ENDPOINT = "https://geocode.googleapis.com/v4beta/geocode/location"

# Rate limit: ~25 QPS (1500/min, leaving headroom for 1800/min limit)
REQUESTS_PER_SECOND = 25
BATCH_SIZE = 100


class RateLimiter:
    """Token bucket rate limiter."""
    def __init__(self, rate: float):
        self.rate = rate  # tokens per second
        self.tokens = rate
        self.last_update = time.monotonic()
        self.lock = asyncio.Lock()
    
    async def acquire(self):
        async with self.lock:
            now = time.monotonic()
            elapsed = now - self.last_update
            self.tokens = min(self.rate, self.tokens + elapsed * self.rate)
            self.last_update = now
            
            if self.tokens < 1:
                wait_time = (1 - self.tokens) / self.rate
                await asyncio.sleep(wait_time)
                self.tokens = 0
            else:
                self.tokens -= 1


async def reverse_geocode_one(session: aiohttp.ClientSession, record: Dict, rate_limiter: RateLimiter) -> Dict:
    """Reverse geocode a single record with rate limiting."""
    await rate_limiter.acquire()
    
    url = f"{V4_ENDPOINT}/{record['lat']},{record['lon']}?key={API_KEY}"
    
    for attempt in range(3):
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status == 429:
                    # Rate limited - wait and retry
                    wait_time = 2 ** (attempt + 1)
                    await asyncio.sleep(wait_time)
                    continue
                    
                if response.status != 200:
                    return {**record, "success": False, "error": f"HTTP {response.status}"}
                
                data = await response.json()
                
                if data.get("results"):
                    result = data["results"][0]
                    address_components = result.get("addressComponents", [])
                    
                    parsed = {
                        "formatted_address": result.get("formattedAddress", ""),
                        "street_number": "",
                        "route": "",
                        "city": "",
                        "state": "",
                        "country": "",
                        "country_code": "",
                        "postcode": ""
                    }
                    
                    for component in address_components:
                        types = component.get("types", [])
                        long_text = component.get("longText", "")
                        short_text = component.get("shortText", "")
                        
                        if "street_number" in types:
                            parsed["street_number"] = long_text
                        elif "route" in types:
                            parsed["route"] = long_text
                        elif "locality" in types:
                            parsed["city"] = long_text
                        elif "administrative_area_level_1" in types:
                            parsed["state"] = short_text
                        elif "country" in types:
                            parsed["country"] = long_text
                            parsed["country_code"] = short_text
                        elif "postal_code" in types:
                            parsed["postcode"] = long_text
                    
                    return {
                        **record,
                        "success": True,
                        "formatted_address": parsed["formatted_address"],
                        "address": f"{parsed['street_number']} {parsed['route']}".strip(),
                        "city": parsed["city"],
                        "state": parsed["state"],
                        "country": parsed["country"],
                        "country_code": parsed["country_code"],
                        "postcode": parsed["postcode"]
                    }
                else:
                    return {**record, "success": False, "error": "No results"}
                    
        except asyncio.TimeoutError:
            if attempt < 2:
                await asyncio.sleep(2 ** (attempt + 1))
                continue
            return {**record, "success": False, "error": "Timeout"}
        except Exception as e:
            if attempt < 2:
                await asyncio.sleep(2 ** (attempt + 1))
                continue
            return {**record, "success": False, "error": str(e)}
    
    return {**record, "success": False, "error": "Max retries"}


async def process_batch(session: aiohttp.ClientSession, records: List[Dict], rate_limiter: RateLimiter) -> List[Dict]:
    """Process a batch of records with rate limiting."""
    tasks = [reverse_geocode_one(session, r, rate_limiter) for r in records]
    return await asyncio.gather(*tasks)


async def main():
    print("=" * 70)
    print("Rate-Limited Async Reverse Geocoding - Google v4 API (FREE Preview)")
    print(f"Target rate: {REQUESTS_PER_SECOND} QPS")
    print("=" * 70)
    print()
    
    # Load unmatched IDs
    unmatched_ids = set()
    with open('output/check_these_again_results.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not row['census_tract_geoid']:
                unmatched_ids.add(row['id'])
    
    print(f"Found {len(unmatched_ids):,} unmatched IDs")
    
    # Load coordinates
    records = []
    with open('check_these_again.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['id'] in unmatched_ids:
                try:
                    records.append({
                        'id': row['id'],
                        'lat': float(row['lat']),
                        'lon': float(row['long'])
                    })
                except:
                    pass
    
    print(f"Loaded {len(records):,} records with coordinates")
    
    # Check for existing progress
    output_file = 'output/unmatched_addresses_geocoded.csv'
    already_done = set()
    if os.path.exists(output_file):
        with open(output_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                already_done.add(row['id'])
        print(f"Found {len(already_done):,} already processed, resuming...")
    
    # Filter out already done
    records = [r for r in records if r['id'] not in already_done]
    print(f"Remaining to process: {len(records):,}")
    
    if not records:
        print("All records already processed!")
        return
    
    eta_minutes = len(records) / REQUESTS_PER_SECOND / 60
    print(f"Estimated time: ~{eta_minutes:.0f} minutes at {REQUESTS_PER_SECOND} QPS")
    print()
    
    # Test API first
    print("Testing API connection...")
    ssl_context = ssl.create_default_context(cafile=certifi.where())
    connector = aiohttp.TCPConnector(ssl=ssl_context, limit=50)
    rate_limiter = RateLimiter(REQUESTS_PER_SECOND)
    
    async with aiohttp.ClientSession(connector=connector) as session:
        test = await reverse_geocode_one(session, {'id': 'test', 'lat': 43.6481, 'lon': -79.3847}, rate_limiter)
        if test.get('success'):
            print(f"✓ API working: {test['formatted_address']}")
        else:
            print(f"✗ API error: {test.get('error')}")
            return
    
    print()
    print(f"Starting processing...")
    print()
    
    # Process in batches
    fieldnames = ['id', 'latitude', 'longitude', 'formatted_address', 'address', 'city', 'state', 'country', 'country_code', 'postcode']
    
    mode = 'a' if already_done else 'w'
    start_time = time.time()
    total_processed = 0
    total_success = 0
    total_failed = 0
    
    connector = aiohttp.TCPConnector(ssl=ssl_context, limit=50)
    async with aiohttp.ClientSession(connector=connector) as session:
        with open(output_file, mode, newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not already_done:
                writer.writeheader()
            
            for i in range(0, len(records), BATCH_SIZE):
                batch = records[i:i+BATCH_SIZE]
                results = await process_batch(session, batch, rate_limiter)
                
                for result in results:
                    if result.get('success'):
                        writer.writerow({
                            'id': result['id'],
                            'latitude': result['lat'],
                            'longitude': result['lon'],
                            'formatted_address': result['formatted_address'],
                            'address': result['address'],
                            'city': result['city'],
                            'state': result['state'],
                            'country': result['country'],
                            'country_code': result['country_code'],
                            'postcode': result['postcode']
                        })
                        total_success += 1
                    else:
                        writer.writerow({
                            'id': result['id'],
                            'latitude': result['lat'],
                            'longitude': result['lon'],
                            'formatted_address': '',
                            'address': '',
                            'city': '',
                            'state': '',
                            'country': '',
                            'country_code': '',
                            'postcode': ''
                        })
                        total_failed += 1
                    
                    total_processed += 1
                
                f.flush()
                
                # Progress update
                elapsed = time.time() - start_time
                rate = total_processed / elapsed if elapsed > 0 else 0
                remaining = (len(records) - total_processed) / rate / 60 if rate > 0 else 0
                pct = (total_processed + len(already_done)) / 79681 * 100
                
                print(f"📊 {total_processed:,}/{len(records):,} ({pct:.1f}%) | {rate:.1f}/sec | ~{remaining:.0f} min | ✓{total_success} ✗{total_failed}")
    
    elapsed = time.time() - start_time
    
    print()
    print("=" * 70)
    print("COMPLETE")
    print("=" * 70)
    print(f"Total processed: {total_processed:,}")
    print(f"✅ Success: {total_success:,}")
    print(f"❌ Failed: {total_failed:,}")
    print(f"Time: {elapsed/60:.1f} minutes ({total_processed/elapsed:.1f} rec/sec)")
    print(f"Output: {output_file}")


if __name__ == "__main__":
    asyncio.run(main())
