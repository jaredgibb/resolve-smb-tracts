#!/usr/bin/env python3
"""
Test Census Bureau Geocoder on unmatched addresses.
Uses reverse geocoding (coordinates -> census tract).
"""

import csv
import requests
import time

def get_tract_from_census_coordinates(lat: float, lon: float) -> dict:
    """
    Query Census Bureau's geocoder with coordinates to get census tract.
    """
    url = "https://geocoding.geo.census.gov/geocoder/geographies/coordinates"
    params = {
        "x": lon,
        "y": lat,
        "benchmark": "Public_AR_Current",
        "vintage": "Current_Current",
        "layers": "Census Tracts",
        "format": "json"
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if data.get("result", {}).get("geographies", {}).get("Census Tracts"):
            tract = data["result"]["geographies"]["Census Tracts"][0]
            return {
                "geoid": tract.get("GEOID", ""),
                "state": tract.get("STATE", ""),
                "county": tract.get("COUNTY", ""),
                "tract": tract.get("TRACT", ""),
                "name": tract.get("NAME", ""),
                "status": "FOUND"
            }
        else:
            return {"status": "NO_TRACT", "geoid": ""}
    except Exception as e:
        return {"status": f"ERROR: {e}", "geoid": ""}


def main():
    # Load the unmatched IDs
    unmatched_ids = set()
    with open('/tmp/unmatched_ids.txt', 'r') as f:
        for line in f:
            id_val = line.strip().rstrip(',')
            if id_val:
                unmatched_ids.add(id_val)
    
    print(f"Loaded {len(unmatched_ids)} unmatched IDs")
    
    # Load coordinates from check_these_again.csv
    records = []
    with open('check_these_again.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['id'] in unmatched_ids:
                records.append({
                    'id': row['id'],
                    'lat': float(row['lat']),
                    'lon': float(row['long'])
                })
    
    print(f"Found {len(records)} records to process")
    print()
    print("Testing Census Bureau Geocoder...")
    print("=" * 70)
    
    found = 0
    not_found = 0
    errors = 0
    
    for i, record in enumerate(records[:100], 1):
        result = get_tract_from_census_coordinates(record['lat'], record['lon'])
        
        status_icon = "✅" if result['geoid'] else "❌"
        
        print(f"{i:3}. ID {record['id']}: ({record['lat']:.4f}, {record['lon']:.4f}) -> {result.get('geoid', 'N/A')} [{result['status']}]")
        
        if result['geoid']:
            found += 1
        elif 'ERROR' in result['status']:
            errors += 1
        else:
            not_found += 1
        
        # Rate limit - be nice to Census API
        time.sleep(0.2)
    
    print()
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Total tested: {min(100, len(records))}")
    print(f"✅ Found tract: {found}")
    print(f"❌ No tract found: {not_found}")
    print(f"⚠️  Errors: {errors}")


if __name__ == "__main__":
    main()
