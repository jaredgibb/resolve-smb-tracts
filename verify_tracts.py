#!/usr/bin/env python3
"""
Verify census tract assignments by comparing our results with Census Bureau's Geocoder API.
Uses the FCC Area API as a secondary verification source.
"""

import csv
import requests
import random
import sys
import time
from datetime import datetime

def get_census_tract_from_coordinates(lat: float, lon: float) -> dict:
    """
    Query the Census Bureau's geocoder to get the census tract for coordinates.
    Uses the TIGERweb REST API.
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
            geoid = tract.get("GEOID", "")
            state = tract.get("STATE", "")
            county = tract.get("COUNTY", "")
            tract_code = tract.get("TRACT", "")
            return {
                "geoid": geoid,
                "state": state,
                "county": county,
                "tract": tract_code,
                "source": "Census Bureau"
            }
    except Exception as e:
        print(f"  Census API error: {e}")
    
    return None


def get_tract_from_fcc(lat: float, lon: float) -> dict:
    """
    Query FCC Area API as a backup verification source.
    """
    url = f"https://geo.fcc.gov/api/census/block/find"
    params = {
        "latitude": lat,
        "longitude": lon,
        "format": "json"
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # FCC returns block FIPS which includes tract
        block_fips = data.get("Block", {}).get("FIPS", "")
        if block_fips and len(block_fips) >= 11:
            # Tract GEOID is first 11 characters of block FIPS
            tract_geoid = block_fips[:11]
            return {
                "geoid": tract_geoid,
                "state": block_fips[:2],
                "county": block_fips[2:5],
                "tract": block_fips[5:11],
                "source": "FCC"
            }
    except Exception as e:
        print(f"  FCC API error: {e}")
    
    return None


def load_sample_records(num_samples: int = 500):
    """
    Load random sample records from address and output files.
    """
    samples = []
    
    # Read from all part files
    part_files = list(range(1, 36))  # 1-35
    samples_per_file = max(1, num_samples // len(part_files))
    
    for part in part_files:
        addr_file = f"addresses_part_{part:03d}.csv"
        tract_file = f"output/tracts_part_{part:03d}.csv"
        
        try:
            # Load addresses
            addresses = {}
            with open(addr_file, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    addresses[row['id']] = row
            
            # Load tracts
            tracts = {}
            with open(tract_file, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    tracts[row['id']] = row['census_tract_geoid']
            
            # Get some random samples from this file
            common_ids = list(set(addresses.keys()) & set(tracts.keys()))
            if common_ids:
                sample_ids = random.sample(common_ids, min(samples_per_file, len(common_ids)))
                for id in sample_ids:
                    addr = addresses[id]
                    samples.append({
                        'id': id,
                        'address': addr.get('address', ''),
                        'city': addr.get('city', ''),
                        'state': addr.get('state', ''),
                        'zipcode': addr.get('zipcode', ''),
                        'latitude': float(addr.get('latitude', 0)),
                        'longitude': float(addr.get('longitude', 0)),
                        'our_tract': tracts[id],
                        'source_file': addr_file
                    })
        except FileNotFoundError:
            print(f"Skipping {addr_file} (not found)")
            continue
        except Exception as e:
            print(f"Error reading {addr_file}: {e}")
            continue
    
    # Shuffle and return requested number
    random.shuffle(samples)
    return samples[:num_samples]


def main():
    output_file = "output/verification_results.csv"
    
    print("=" * 70)
    print("Census Tract Verification (500 samples)")
    print("=" * 70)
    print()
    print("Loading sample records...")
    
    samples = load_sample_records(500)
    
    if not samples:
        print("No samples found!")
        return
    
    print(f"Loaded {len(samples)} sample records")
    print(f"Results will be saved to: {output_file}")
    print()
    
    matches = 0
    mismatches = 0
    errors = 0
    
    results = []
    start_time = time.time()
    
    for i, sample in enumerate(samples, 1):
        # Progress update every 50 records
        if i % 50 == 0 or i == 1:
            elapsed = time.time() - start_time
            rate = i / elapsed if elapsed > 0 else 0
            eta = (len(samples) - i) / rate if rate > 0 else 0
            print(f"Progress: {i}/{len(samples)} ({i/len(samples)*100:.1f}%) | {rate:.1f}/sec | ETA: {eta:.0f}s")
        
        # Try Census Bureau API first
        result = get_census_tract_from_coordinates(sample['latitude'], sample['longitude'])
        
        # Fall back to FCC if Census fails
        if not result:
            result = get_tract_from_fcc(sample['latitude'], sample['longitude'])
        
        # Determine status
        if result:
            api_tract = result['geoid']
            source = result['source']
            if result['geoid'] == sample['our_tract']:
                status = "MATCH"
                matches += 1
            else:
                status = "MISMATCH"
                mismatches += 1
        else:
            api_tract = ""
            source = ""
            status = "ERROR"
            errors += 1
        
        results.append({
            'id': sample['id'],
            'address': sample['address'],
            'city': sample['city'],
            'state': sample['state'],
            'zipcode': sample['zipcode'],
            'latitude': sample['latitude'],
            'longitude': sample['longitude'],
            'our_tract': sample['our_tract'],
            'api_tract': api_tract,
            'api_source': source,
            'status': status,
            'source_file': sample['source_file']
        })
        
        # Small delay to avoid rate limiting
        time.sleep(0.1)
    
    # Write results to CSV
    print()
    print(f"Writing results to {output_file}...")
    
    with open(output_file, 'w', newline='') as f:
        fieldnames = ['id', 'address', 'city', 'state', 'zipcode', 'latitude', 'longitude', 
                      'our_tract', 'api_tract', 'api_source', 'status', 'source_file']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    
    elapsed_total = time.time() - start_time
    
    print()
    print("=" * 70)
    print("VERIFICATION SUMMARY")
    print("=" * 70)
    print(f"Total samples: {len(samples)}")
    print(f"✅ Matches: {matches}")
    print(f"❌ Mismatches: {mismatches}")
    print(f"⚠️  Errors/Unavailable: {errors}")
    print(f"Time elapsed: {elapsed_total:.1f}s")
    
    if matches + mismatches > 0:
        accuracy = matches / (matches + mismatches) * 100
        print(f"\nAccuracy: {accuracy:.2f}%")
    
    print(f"\nResults saved to: {output_file}")
    print()


if __name__ == "__main__":
    main()
