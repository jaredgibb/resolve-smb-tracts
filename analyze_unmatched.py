#!/usr/bin/env python3
"""
Analyze unmatched addresses by location to identify where they are.
"""

import csv
from collections import defaultdict

def get_country_region(lat: float, lon: float) -> str:
    """
    Rough estimation of country/region based on lat/lon bounds.
    """
    # US bounds (continental)
    if 24.5 <= lat <= 49.5 and -125 <= lon <= -66.5:
        return "US_CONTINENTAL"
    
    # Alaska
    if 51 <= lat <= 72 and -180 <= lon <= -129:
        return "US_ALASKA"
    
    # Hawaii
    if 18.5 <= lat <= 22.5 and -161 <= lon <= -154:
        return "US_HAWAII"
    
    # Puerto Rico
    if 17.9 <= lat <= 18.6 and -67.5 <= lon <= -65.2:
        return "US_PUERTO_RICO"
    
    # US Virgin Islands
    if 17.6 <= lat <= 18.5 and -65.2 <= lon <= -64.5:
        return "US_VIRGIN_ISLANDS"
    
    # Guam
    if 13.2 <= lat <= 13.7 and 144.6 <= lon <= 145:
        return "US_GUAM"
    
    # American Samoa
    if -14.5 <= lat <= -14 and -171 <= lon <= -169:
        return "US_AMERICAN_SAMOA"
    
    # Canada
    if 41.5 <= lat <= 84 and -141 <= lon <= -52:
        return "CANADA"
    
    # Mexico
    if 14 <= lat <= 33 and -118 <= lon <= -86:
        return "MEXICO"
    
    # Caribbean (general)
    if 10 <= lat <= 27 and -90 <= lon <= -59:
        return "CARIBBEAN"
    
    # Central America
    if 7 <= lat <= 18 and -93 <= lon <= -77:
        return "CENTRAL_AMERICA"
    
    # South America
    if -56 <= lat <= 13 and -82 <= lon <= -34:
        return "SOUTH_AMERICA"
    
    # Europe
    if 35 <= lat <= 72 and -25 <= lon <= 65:
        return "EUROPE"
    
    # Asia
    if -10 <= lat <= 80 and 25 <= lon <= 180:
        return "ASIA"
    
    # Africa
    if -35 <= lat <= 38 and -18 <= lon <= 52:
        return "AFRICA"
    
    # Australia/Oceania
    if -50 <= lat <= 0 and 110 <= lon <= 180:
        return "AUSTRALIA_OCEANIA"
    
    return "UNKNOWN"


def main():
    # Load unmatched addresses from check_these_again.csv and results
    unmatched_ids = set()
    with open('output/check_these_again_results.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not row['census_tract_geoid']:
                unmatched_ids.add(row['id'])
    
    print(f"Total unmatched: {len(unmatched_ids)}")
    print()
    
    # Analyze locations
    regions = defaultdict(list)
    
    with open('check_these_again.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['id'] in unmatched_ids:
                try:
                    lat = float(row['lat'])
                    lon = float(row['long'])
                    region = get_country_region(lat, lon)
                    regions[region].append({
                        'id': row['id'],
                        'lat': lat,
                        'lon': lon
                    })
                except:
                    regions['INVALID_COORDS'].append({'id': row['id'], 'lat': 0, 'lon': 0})
    
    print("=" * 60)
    print("UNMATCHED ADDRESSES BY REGION")
    print("=" * 60)
    
    # Sort by count
    sorted_regions = sorted(regions.items(), key=lambda x: len(x[1]), reverse=True)
    
    for region, addresses in sorted_regions:
        count = len(addresses)
        pct = count / len(unmatched_ids) * 100
        print(f"{region:25} {count:>8,} ({pct:5.1f}%)")
        
        # Show sample coordinates for each region
        if addresses[:3]:
            samples = addresses[:3]
            sample_str = ", ".join([f"({a['lat']:.2f}, {a['lon']:.2f})" for a in samples])
            print(f"  Samples: {sample_str}")
    
    print()
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    us_regions = ['US_CONTINENTAL', 'US_ALASKA', 'US_HAWAII', 'US_PUERTO_RICO', 
                  'US_VIRGIN_ISLANDS', 'US_GUAM', 'US_AMERICAN_SAMOA']
    
    us_count = sum(len(regions[r]) for r in us_regions if r in regions)
    intl_count = len(unmatched_ids) - us_count
    
    print(f"US locations (should have tracts): {us_count:,}")
    print(f"International (no US tracts):      {intl_count:,}")


if __name__ == "__main__":
    main()
