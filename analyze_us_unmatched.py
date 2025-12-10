#!/usr/bin/env python3
"""
Deep dive into unmatched US Continental addresses to understand why they didn't match.
"""

import csv
import requests
import time
from collections import defaultdict

def get_census_tract_api(lat: float, lon: float) -> dict:
    """Query Census Bureau API for this coordinate."""
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
                "status": "FOUND",
                "geoid": tract.get("GEOID", ""),
                "state": tract.get("STATE", ""),
                "county": tract.get("COUNTY", ""),
            }
        return {"status": "NO_TRACT", "geoid": ""}
    except Exception as e:
        return {"status": f"ERROR", "geoid": ""}


def analyze_us_continental():
    # Load unmatched IDs
    unmatched_ids = set()
    with open('output/check_these_again_results.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not row['census_tract_geoid']:
                unmatched_ids.add(row['id'])
    
    # Filter to US Continental only
    us_continental = []
    with open('check_these_again.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['id'] in unmatched_ids:
                try:
                    lat = float(row['lat'])
                    lon = float(row['long'])
                    # US Continental bounds
                    if 24.5 <= lat <= 49.5 and -125 <= lon <= -66.5:
                        us_continental.append({
                            'id': row['id'],
                            'lat': lat,
                            'lon': lon
                        })
                except:
                    pass
    
    print(f"Total US Continental unmatched: {len(us_continental)}")
    print()
    
    # Analyze by sub-region (state-ish areas based on lon)
    regions = defaultdict(list)
    for addr in us_continental:
        lat, lon = addr['lat'], addr['lon']
        
        # Categorize by rough region
        if lon < -114:
            region = "Pacific Coast (CA, OR, WA)"
        elif lon < -103:
            region = "Mountain West (NV, AZ, UT, CO, NM)"
        elif lon < -94:
            region = "Central (TX, OK, KS, NE, SD, ND)"
        elif lon < -87:
            region = "Midwest (MN, IA, MO, AR, LA)"
        elif lon < -80:
            region = "Great Lakes/South (WI, IL, IN, MI, OH, KY, TN, MS, AL)"
        elif lon < -75:
            region = "Southeast (GA, FL, SC, NC, VA, WV)"
        else:
            region = "Northeast (PA, NY, NJ, New England)"
        
        # Special case: South Texas border
        if lat < 27 and lon > -100 and lon < -96:
            region = "South Texas Border"
        
        regions[region].append(addr)
    
    print("=" * 70)
    print("US CONTINENTAL UNMATCHED BY REGION")
    print("=" * 70)
    
    for region, addrs in sorted(regions.items(), key=lambda x: -len(x[1])):
        print(f"\n{region}: {len(addrs):,}")
        # Sample coords
        samples = addrs[:5]
        for s in samples:
            print(f"  ({s['lat']:.4f}, {s['lon']:.4f})")
    
    # Test a sample with Census API
    print()
    print("=" * 70)
    print("TESTING 50 SAMPLES WITH CENSUS BUREAU API")
    print("=" * 70)
    
    import random
    test_sample = random.sample(us_continental, min(50, len(us_continental)))
    
    api_found = 0
    api_not_found = 0
    
    found_examples = []
    not_found_examples = []
    
    for i, addr in enumerate(test_sample, 1):
        result = get_census_tract_api(addr['lat'], addr['lon'])
        
        if result['geoid']:
            api_found += 1
            found_examples.append((addr, result))
        else:
            api_not_found += 1
            not_found_examples.append(addr)
        
        if i % 10 == 0:
            print(f"  Tested {i}/50...")
        
        time.sleep(0.15)
    
    print()
    print(f"Census API found tract: {api_found}")
    print(f"Census API NO tract:    {api_not_found}")
    
    if found_examples:
        print()
        print("Examples where Census API found a tract (our shapefiles missed):")
        for addr, result in found_examples[:10]:
            print(f"  ID {addr['id']}: ({addr['lat']:.4f}, {addr['lon']:.4f}) -> {result['geoid']}")
    
    if not_found_examples:
        print()
        print("Examples where even Census API found NO tract:")
        for addr in not_found_examples[:10]:
            print(f"  ID {addr['id']}: ({addr['lat']:.4f}, {addr['lon']:.4f})")


if __name__ == "__main__":
    analyze_us_continental()
