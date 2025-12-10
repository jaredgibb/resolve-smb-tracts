#!/usr/bin/env python3
"""
Reverse geocode the ~80k unmatched addresses using multiple free services.
Uses Nominatim (OSM) + Photon for faster processing.
Output: id, lat, lon, address, city, state, country
"""

import csv
import requests
import time
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Nominatim requires a user agent
HEADERS = {
    "User-Agent": "CensusTractGeocoder/1.0 (census-tract-lookup@example.com)"
}

# Rate limiting
nominatim_lock = threading.Lock()
last_nominatim_call = [0]

def reverse_geocode_photon(lat: float, lon: float) -> Optional[dict]:
    """
    Reverse geocode using Photon (OSM-based, faster, no strict rate limit).
    """
    url = "https://photon.komoot.io/reverse"
    params = {
        "lat": lat,
        "lon": lon
    }
    
    try:
        response = requests.get(url, params=params, headers=HEADERS, timeout=15)
        response.raise_for_status()
        data = response.json()
        
        if data.get("features"):
            props = data["features"][0].get("properties", {})
            return {
                "display_name": props.get("name", ""),
                "road": props.get("street", ""),
                "house_number": props.get("housenumber", ""),
                "city": props.get("city", props.get("town", props.get("village", ""))),
                "state": props.get("state", ""),
                "country": props.get("country", ""),
                "country_code": props.get("countrycode", "").upper(),
                "postcode": props.get("postcode", ""),
                "source": "Photon"
            }
    except:
        pass
    return None


def reverse_geocode_nominatim(lat: float, lon: float) -> Optional[dict]:
    """
    Reverse geocode using Nominatim (OpenStreetMap).
    Rate limited to 1 req/sec.
    """
    # Rate limiting
    with nominatim_lock:
        now = time.time()
        elapsed = now - last_nominatim_call[0]
        if elapsed < 1.0:
            time.sleep(1.0 - elapsed)
        last_nominatim_call[0] = time.time()
    
    url = "https://nominatim.openstreetmap.org/reverse"
    params = {
        "lat": lat,
        "lon": lon,
        "format": "json",
        "addressdetails": 1
    }
    
    try:
        response = requests.get(url, params=params, headers=HEADERS, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if "error" not in data:
            address = data.get("address", {})
            return {
                "display_name": data.get("display_name", ""),
                "road": address.get("road", address.get("street", "")),
                "house_number": address.get("house_number", ""),
                "city": address.get("city", address.get("town", address.get("village", address.get("municipality", "")))),
                "state": address.get("state", address.get("province", "")),
                "country": address.get("country", ""),
                "country_code": address.get("country_code", "").upper(),
                "postcode": address.get("postcode", ""),
                "source": "Nominatim"
            }
    except:
        pass
    return None


def geocode_record(record: dict) -> dict:
    """Geocode a single record using Photon first, then Nominatim as fallback."""
    lat, lon = record['lat'], record['lon']
    
    # Try Photon first (faster)
    result = reverse_geocode_photon(lat, lon)
    
    # Fallback to Nominatim if Photon fails
    if not result:
        result = reverse_geocode_nominatim(lat, lon)
    
    if result:
        return {
            'id': record['id'],
            'latitude': lat,
            'longitude': lon,
            'address': f"{result['house_number']} {result['road']}".strip(),
            'city': result['city'],
            'state': result['state'],
            'country': result['country'],
            'country_code': result['country_code'],
            'postcode': result['postcode'],
            'full_address': result['display_name'],
            'source': result['source']
        }
    else:
        return {
            'id': record['id'],
            'latitude': lat,
            'longitude': lon,
            'address': '',
            'city': '',
            'state': '',
            'country': '',
            'country_code': '',
            'postcode': '',
            'full_address': '',
            'source': 'FAILED'
        }


def main():
    print("=" * 70)
    print("Reverse Geocoding Unmatched Addresses (Multi-Service)")
    print("=" * 70)
    print()
    
    # Load unmatched IDs (those with blank census_tract_geoid)
    unmatched_ids = set()
    with open('output/check_these_again_results.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not row['census_tract_geoid']:
                unmatched_ids.add(row['id'])
    
    print(f"Found {len(unmatched_ids):,} unmatched IDs")
    
    # Load coordinates for unmatched IDs
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
    print("Using Photon (primary) - ~10 req/sec")
    print(f"⏱️  Estimated time: ~{len(records) / 10 / 60:.0f} minutes")
    print()
    print("Starting reverse geocoding...")
    print()
    
    # Output file
    output_file = 'output/unmatched_addresses_geocoded.csv'
    
    with open(output_file, 'w', newline='') as f:
        fieldnames = ['id', 'latitude', 'longitude', 'address', 'city', 'state', 'country', 'country_code', 'postcode', 'full_address', 'source']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        start_time = time.time()
        success = 0
        failed = 0
        
        for i, record in enumerate(records, 1):
            lat, lon = record['lat'], record['lon']
            
            # Try Photon first (faster)
            result = reverse_geocode_photon(lat, lon)
            
            if result:
                writer.writerow({
                    'id': record['id'],
                    'latitude': lat,
                    'longitude': lon,
                    'address': f"{result['house_number']} {result['road']}".strip(),
                    'city': result['city'],
                    'state': result['state'],
                    'country': result['country'],
                    'country_code': result['country_code'],
                    'postcode': result['postcode'],
                    'full_address': result['display_name'],
                    'source': result['source']
                })
                success += 1
            else:
                writer.writerow({
                    'id': record['id'],
                    'latitude': lat,
                    'longitude': lon,
                    'address': '',
                    'city': '',
                    'state': '',
                    'country': '',
                    'country_code': '',
                    'postcode': '',
                    'full_address': '',
                    'source': 'FAILED'
                })
                failed += 1
            
            # Progress every 500 records
            if i % 500 == 0:
                elapsed = time.time() - start_time
                rate = i / elapsed if elapsed > 0 else 0
                remaining = (len(records) - i) / rate / 60 if rate > 0 else 0
                print(f"📊 Progress: {i:,}/{len(records):,} ({i/len(records)*100:.1f}%) | {rate:.1f}/sec | ~{remaining:.0f} min remaining")
                f.flush()
            
            # Rate limit - ~10/sec for Photon
            time.sleep(0.1)
    
    elapsed = time.time() - start_time
    
    print()
    print("=" * 70)
    print("COMPLETE")
    print("=" * 70)
    print(f"Total processed: {len(records):,}")
    print(f"✅ Geocoded: {success:,}")
    print(f"❌ Failed: {failed:,}")
    print(f"Time: {elapsed/60:.1f} minutes")
    print(f"Output: {output_file}")


if __name__ == "__main__":
    main()
