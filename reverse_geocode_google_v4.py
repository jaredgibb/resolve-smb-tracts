#!/usr/bin/env python3
"""
Reverse geocode unmatched addresses using Google Geocoding API v4 (FREE preview).
Uses the v4beta endpoint which has $0 cost during preview.
"""

import csv
import os
import requests
import time

# Google Geocoding API v4 (FREE during preview)
API_KEY = os.environ.get("GOOGLE_MAPS_API_KEY")
if not API_KEY:
    raise RuntimeError("GOOGLE_MAPS_API_KEY env var required for Google Geocoding API calls")
V4_ENDPOINT = "https://geocode.googleapis.com/v4beta/geocode/location"


def reverse_geocode_google_v4(lat: float, lon: float, retries: int = 3) -> dict:
    """
    Reverse geocode using Google Geocoding API v4 (FREE preview).
    """
    # Key must be in URL directly, not as a param (requests encoding issue)
    url = f"{V4_ENDPOINT}/{lat},{lon}?key={API_KEY}"
    
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=30)
            
            if response.status_code == 429:
                # Rate limited - wait and retry
                wait_time = 2 ** (attempt + 1)  # 2, 4, 8 seconds
                time.sleep(wait_time)
                continue
                
            response.raise_for_status()
            data = response.json()
            
            if data.get("results"):
                result = data["results"][0]
                
                # Extract address components
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
                return {"success": False, "error": "No results"}
                
        except requests.exceptions.HTTPError as e:
            return {"success": False, "error": f"HTTP {e.response.status_code}"}
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2 ** (attempt + 1))
                continue
            return {"success": False, "error": str(e)}
    
    return {"success": False, "error": "Max retries exceeded"}


def main():
    print("=" * 70)
    print("Reverse Geocoding with Google Geocoding API v4 (FREE Preview)")
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
    print()
    
    # Test API first
    print("Testing API connection...")
    test_result = reverse_geocode_google_v4(43.6481, -79.3847)
    if test_result['success']:
        print(f"✓ API working: {test_result['formatted_address']}")
    else:
        print(f"✗ API error: {test_result['error']}")
        return
    
    print()
    print("Starting reverse geocoding...")
    print()
    
    # Output file
    output_file = 'output/unmatched_addresses_geocoded.csv'
    
    # Check for existing progress
    already_done = set()
    import os
    if os.path.exists(output_file):
        with open(output_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                already_done.add(row['id'])
        print(f"Found {len(already_done):,} already processed, resuming...")
    
    # Filter out already done
    records = [r for r in records if r['id'] not in already_done]
    print(f"Remaining to process: {len(records):,}")
    print()
    
    mode = 'a' if already_done else 'w'
    with open(output_file, mode, newline='') as f:
        fieldnames = ['id', 'latitude', 'longitude', 'formatted_address', 'address', 'city', 'state', 'country', 'country_code', 'postcode']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not already_done:
            writer.writeheader()
        
        start_time = time.time()
        success = 0
        failed = 0
        
        for i, record in enumerate(records, 1):
            result = reverse_geocode_google_v4(record['lat'], record['lon'])
            
            if result['success']:
                writer.writerow({
                    'id': record['id'],
                    'latitude': record['lat'],
                    'longitude': record['lon'],
                    'formatted_address': result['formatted_address'],
                    'address': result['address'],
                    'city': result['city'],
                    'state': result['state'],
                    'country': result['country'],
                    'country_code': result['country_code'],
                    'postcode': result['postcode']
                })
                success += 1
            else:
                writer.writerow({
                    'id': record['id'],
                    'latitude': record['lat'],
                    'longitude': record['lon'],
                    'formatted_address': '',
                    'address': '',
                    'city': '',
                    'state': '',
                    'country': '',
                    'country_code': '',
                    'postcode': ''
                })
                failed += 1
            
            # Progress every 500 records
            if i % 500 == 0:
                elapsed = time.time() - start_time
                rate = i / elapsed if elapsed > 0 else 0
                remaining = (len(records) - i) / rate / 60 if rate > 0 else 0
                print(f"📊 Progress: {i:,}/{len(records):,} ({i/len(records)*100:.1f}%) | {rate:.1f}/sec | ~{remaining:.0f} min remaining | ✓{success} ✗{failed}")
                f.flush()
            
            # Minimal delay - Google v4 handles high rates
            # But stay under ~25/sec to avoid rate limits (1800/min = 30/sec, leave headroom)
            # Using 10/sec for overnight reliability (~2 hours for 72k)
            time.sleep(0.1)
    
    elapsed = time.time() - start_time
    
    print()
    print("=" * 70)
    print("COMPLETE")
    print("=" * 70)
    print(f"Total processed: {len(records):,}")
    print(f"✅ Success: {success:,}")
    print(f"❌ Failed: {failed:,}")
    print(f"Time: {elapsed/60:.1f} minutes")
    print(f"Output: {output_file}")


if __name__ == "__main__":
    main()
