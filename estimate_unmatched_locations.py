#!/usr/bin/env python3
"""
Option C: Fast country/region estimation for unmatched addresses based on lat/long.
No API calls - uses geographic bounding boxes.
Outputs:
  1. output/unmatched_addresses_estimated.csv - The data
  2. output/unmatched_addresses_methodology.md - Explanation of how decisions were made
"""

import csv

def get_location_estimate(lat: float, lon: float) -> dict:
    """
    Estimate country/region based on lat/lon bounding boxes.
    Returns dict with country, region, confidence, and reasoning.
    """
    
    # ========== US TERRITORIES (most specific first) ==========
    
    # Puerto Rico
    if 17.9 <= lat <= 18.6 and -67.5 <= lon <= -65.2:
        return {
            "country": "United States",
            "country_code": "US",
            "region": "Puerto Rico",
            "confidence": "HIGH",
            "reasoning": "Coordinates within Puerto Rico bounding box (17.9-18.6°N, 67.5-65.2°W)"
        }
    
    # US Virgin Islands
    if 17.6 <= lat <= 18.5 and -65.2 <= lon <= -64.5:
        return {
            "country": "United States",
            "country_code": "US",
            "region": "US Virgin Islands",
            "confidence": "HIGH",
            "reasoning": "Coordinates within US Virgin Islands bounding box (17.6-18.5°N, 65.2-64.5°W)"
        }
    
    # Guam
    if 13.2 <= lat <= 13.7 and 144.6 <= lon <= 145.0:
        return {
            "country": "United States",
            "country_code": "US",
            "region": "Guam",
            "confidence": "HIGH",
            "reasoning": "Coordinates within Guam bounding box (13.2-13.7°N, 144.6-145.0°E)"
        }
    
    # American Samoa
    if -14.5 <= lat <= -14.0 and -171.0 <= lon <= -169.0:
        return {
            "country": "United States",
            "country_code": "US",
            "region": "American Samoa",
            "confidence": "HIGH",
            "reasoning": "Coordinates within American Samoa bounding box (14.0-14.5°S, 169.0-171.0°W)"
        }
    
    # Northern Mariana Islands
    if 14.0 <= lat <= 20.6 and 144.8 <= lon <= 146.1:
        return {
            "country": "United States",
            "country_code": "US",
            "region": "Northern Mariana Islands",
            "confidence": "HIGH",
            "reasoning": "Coordinates within CNMI bounding box (14.0-20.6°N, 144.8-146.1°E)"
        }
    
    # ========== CANADA (before US to catch border areas) ==========
    
    # Southern Ontario (Toronto, Ottawa, etc.) - VERY specific
    if 41.5 <= lat <= 46.0 and -83.5 <= lon <= -74.5:
        return {
            "country": "Canada",
            "country_code": "CA",
            "region": "Ontario",
            "confidence": "HIGH",
            "reasoning": "Coordinates in Southern Ontario region (41.5-46.0°N, 83.5-74.5°W) - includes Toronto, Ottawa, Windsor"
        }
    
    # Quebec
    if 45.0 <= lat <= 52.0 and -79.5 <= lon <= -64.0:
        return {
            "country": "Canada",
            "country_code": "CA",
            "region": "Quebec",
            "confidence": "HIGH",
            "reasoning": "Coordinates in Quebec region (45.0-52.0°N, 79.5-64.0°W) - includes Montreal, Quebec City"
        }
    
    # British Columbia
    if 48.3 <= lat <= 60.0 and -139.0 <= lon <= -114.0:
        return {
            "country": "Canada",
            "country_code": "CA",
            "region": "British Columbia",
            "confidence": "HIGH",
            "reasoning": "Coordinates in British Columbia (48.3-60.0°N, 139.0-114.0°W) - includes Vancouver, Victoria"
        }
    
    # Alberta
    if 49.0 <= lat <= 60.0 and -120.0 <= lon <= -110.0:
        return {
            "country": "Canada",
            "country_code": "CA",
            "region": "Alberta",
            "confidence": "HIGH",
            "reasoning": "Coordinates in Alberta (49.0-60.0°N, 120.0-110.0°W) - includes Calgary, Edmonton"
        }
    
    # Manitoba
    if 49.0 <= lat <= 60.0 and -102.0 <= lon <= -95.0:
        return {
            "country": "Canada",
            "country_code": "CA",
            "region": "Manitoba",
            "confidence": "HIGH",
            "reasoning": "Coordinates in Manitoba (49.0-60.0°N, 102.0-95.0°W) - includes Winnipeg"
        }
    
    # Saskatchewan
    if 49.0 <= lat <= 60.0 and -110.0 <= lon <= -102.0:
        return {
            "country": "Canada",
            "country_code": "CA",
            "region": "Saskatchewan",
            "confidence": "HIGH",
            "reasoning": "Coordinates in Saskatchewan (49.0-60.0°N, 110.0-102.0°W)"
        }
    
    # General Canada (rest)
    if 41.5 <= lat <= 84.0 and -141.0 <= lon <= -52.0:
        return {
            "country": "Canada",
            "country_code": "CA",
            "region": "Canada (general)",
            "confidence": "MEDIUM",
            "reasoning": "Coordinates within general Canada bounding box (41.5-84.0°N, 141.0-52.0°W)"
        }
    
    # ========== MEXICO ==========
    
    # Baja California
    if 28.0 <= lat <= 33.0 and -118.0 <= lon <= -114.0:
        return {
            "country": "Mexico",
            "country_code": "MX",
            "region": "Baja California",
            "confidence": "HIGH",
            "reasoning": "Coordinates in Baja California (28.0-33.0°N, 118.0-114.0°W) - includes Tijuana, Mexicali"
        }
    
    # Baja California Sur
    if 22.8 <= lat <= 28.0 and -115.0 <= lon <= -109.0:
        return {
            "country": "Mexico",
            "country_code": "MX",
            "region": "Baja California Sur",
            "confidence": "HIGH",
            "reasoning": "Coordinates in Baja California Sur (22.8-28.0°N, 115.0-109.0°W)"
        }
    
    # Sonora
    if 26.5 <= lat <= 32.5 and -115.0 <= lon <= -108.5:
        return {
            "country": "Mexico",
            "country_code": "MX",
            "region": "Sonora",
            "confidence": "HIGH",
            "reasoning": "Coordinates in Sonora state (26.5-32.5°N, 115.0-108.5°W)"
        }
    
    # Chihuahua
    if 25.5 <= lat <= 31.8 and -109.0 <= lon <= -103.3:
        return {
            "country": "Mexico",
            "country_code": "MX",
            "region": "Chihuahua",
            "confidence": "HIGH",
            "reasoning": "Coordinates in Chihuahua state (25.5-31.8°N, 109.0-103.3°W)"
        }
    
    # Nuevo León (Monterrey area)
    if 23.0 <= lat <= 27.8 and -101.5 <= lon <= -98.5:
        return {
            "country": "Mexico",
            "country_code": "MX",
            "region": "Nuevo León",
            "confidence": "HIGH",
            "reasoning": "Coordinates in Nuevo León (23.0-27.8°N, 101.5-98.5°W) - includes Monterrey"
        }
    
    # Tamaulipas (south Texas border)
    if 22.0 <= lat <= 27.7 and -100.2 <= lon <= -97.0:
        return {
            "country": "Mexico",
            "country_code": "MX",
            "region": "Tamaulipas",
            "confidence": "HIGH",
            "reasoning": "Coordinates in Tamaulipas (22.0-27.7°N, 100.2-97.0°W) - south of Texas border"
        }
    
    # Yucatan Peninsula
    if 19.5 <= lat <= 21.7 and -91.0 <= lon <= -86.7:
        return {
            "country": "Mexico",
            "country_code": "MX",
            "region": "Yucatan Peninsula",
            "confidence": "HIGH",
            "reasoning": "Coordinates in Yucatan Peninsula (19.5-21.7°N, 91.0-86.7°W) - includes Merida, Cancun"
        }
    
    # Sinaloa (Mazatlan area)
    if 22.5 <= lat <= 27.0 and -109.5 <= lon <= -105.5:
        return {
            "country": "Mexico",
            "country_code": "MX",
            "region": "Sinaloa",
            "confidence": "HIGH",
            "reasoning": "Coordinates in Sinaloa (22.5-27.0°N, 109.5-105.5°W) - includes Mazatlan"
        }
    
    # General Mexico
    if 14.5 <= lat <= 33.0 and -118.5 <= lon <= -86.5:
        return {
            "country": "Mexico",
            "country_code": "MX",
            "region": "Mexico (general)",
            "confidence": "MEDIUM",
            "reasoning": "Coordinates within general Mexico bounding box (14.5-33.0°N, 118.5-86.5°W)"
        }
    
    # ========== EUROPE ==========
    
    # United Kingdom
    if 49.9 <= lat <= 60.9 and -8.2 <= lon <= 1.8:
        return {
            "country": "United Kingdom",
            "country_code": "GB",
            "region": "United Kingdom",
            "confidence": "HIGH",
            "reasoning": "Coordinates within UK bounding box (49.9-60.9°N, 8.2°W-1.8°E)"
        }
    
    # Ireland
    if 51.4 <= lat <= 55.4 and -10.5 <= lon <= -5.5:
        return {
            "country": "Ireland",
            "country_code": "IE",
            "region": "Ireland",
            "confidence": "HIGH",
            "reasoning": "Coordinates within Ireland bounding box (51.4-55.4°N, 10.5-5.5°W)"
        }
    
    # Germany
    if 47.3 <= lat <= 55.1 and 5.9 <= lon <= 15.0:
        return {
            "country": "Germany",
            "country_code": "DE",
            "region": "Germany",
            "confidence": "HIGH",
            "reasoning": "Coordinates within Germany bounding box (47.3-55.1°N, 5.9-15.0°E)"
        }
    
    # France
    if 41.3 <= lat <= 51.1 and -5.2 <= lon <= 9.6:
        return {
            "country": "France",
            "country_code": "FR",
            "region": "France",
            "confidence": "HIGH",
            "reasoning": "Coordinates within France bounding box (41.3-51.1°N, 5.2°W-9.6°E)"
        }
    
    # Spain
    if 36.0 <= lat <= 43.8 and -9.3 <= lon <= 4.3:
        return {
            "country": "Spain",
            "country_code": "ES",
            "region": "Spain",
            "confidence": "HIGH",
            "reasoning": "Coordinates within Spain bounding box (36.0-43.8°N, 9.3°W-4.3°E)"
        }
    
    # Italy
    if 36.6 <= lat <= 47.1 and 6.6 <= lon <= 18.5:
        return {
            "country": "Italy",
            "country_code": "IT",
            "region": "Italy",
            "confidence": "HIGH",
            "reasoning": "Coordinates within Italy bounding box (36.6-47.1°N, 6.6-18.5°E)"
        }
    
    # Netherlands
    if 50.8 <= lat <= 53.5 and 3.4 <= lon <= 7.2:
        return {
            "country": "Netherlands",
            "country_code": "NL",
            "region": "Netherlands",
            "confidence": "HIGH",
            "reasoning": "Coordinates within Netherlands bounding box (50.8-53.5°N, 3.4-7.2°E)"
        }
    
    # General Europe
    if 35.0 <= lat <= 72.0 and -25.0 <= lon <= 65.0:
        return {
            "country": "Europe",
            "country_code": "EU",
            "region": "Europe (general)",
            "confidence": "MEDIUM",
            "reasoning": "Coordinates within general Europe bounding box (35.0-72.0°N, 25.0°W-65.0°E)"
        }
    
    # ========== ASIA ==========
    
    # India
    if 6.7 <= lat <= 35.5 and 68.1 <= lon <= 97.4:
        return {
            "country": "India",
            "country_code": "IN",
            "region": "India",
            "confidence": "HIGH",
            "reasoning": "Coordinates within India bounding box (6.7-35.5°N, 68.1-97.4°E)"
        }
    
    # UAE
    if 22.6 <= lat <= 26.1 and 51.5 <= lon <= 56.4:
        return {
            "country": "United Arab Emirates",
            "country_code": "AE",
            "region": "UAE",
            "confidence": "HIGH",
            "reasoning": "Coordinates within UAE bounding box (22.6-26.1°N, 51.5-56.4°E)"
        }
    
    # Philippines
    if 4.6 <= lat <= 21.1 and 116.9 <= lon <= 126.6:
        return {
            "country": "Philippines",
            "country_code": "PH",
            "region": "Philippines",
            "confidence": "HIGH",
            "reasoning": "Coordinates within Philippines bounding box (4.6-21.1°N, 116.9-126.6°E)"
        }
    
    # Japan
    if 24.0 <= lat <= 46.0 and 122.9 <= lon <= 146.0:
        return {
            "country": "Japan",
            "country_code": "JP",
            "region": "Japan",
            "confidence": "HIGH",
            "reasoning": "Coordinates within Japan bounding box (24.0-46.0°N, 122.9-146.0°E)"
        }
    
    # China
    if 18.2 <= lat <= 53.6 and 73.5 <= lon <= 135.0:
        return {
            "country": "China",
            "country_code": "CN",
            "region": "China",
            "confidence": "MEDIUM",
            "reasoning": "Coordinates within China bounding box (18.2-53.6°N, 73.5-135.0°E)"
        }
    
    # General Asia
    if -10.0 <= lat <= 80.0 and 25.0 <= lon <= 180.0:
        return {
            "country": "Asia",
            "country_code": "AS",
            "region": "Asia (general)",
            "confidence": "LOW",
            "reasoning": "Coordinates within general Asia bounding box"
        }
    
    # ========== AUSTRALIA / OCEANIA ==========
    
    # Australia
    if -44.0 <= lat <= -10.0 and 112.0 <= lon <= 154.0:
        return {
            "country": "Australia",
            "country_code": "AU",
            "region": "Australia",
            "confidence": "HIGH",
            "reasoning": "Coordinates within Australia bounding box (10.0-44.0°S, 112.0-154.0°E)"
        }
    
    # New Zealand
    if -47.5 <= lat <= -34.0 and 166.0 <= lon <= 179.0:
        return {
            "country": "New Zealand",
            "country_code": "NZ",
            "region": "New Zealand",
            "confidence": "HIGH",
            "reasoning": "Coordinates within New Zealand bounding box (34.0-47.5°S, 166.0-179.0°E)"
        }
    
    # General Oceania
    if -50.0 <= lat <= 0.0 and 110.0 <= lon <= 180.0:
        return {
            "country": "Oceania",
            "country_code": "OC",
            "region": "Oceania (general)",
            "confidence": "LOW",
            "reasoning": "Coordinates within general Oceania region"
        }
    
    # ========== SOUTH AMERICA ==========
    
    # Brazil
    if -33.8 <= lat <= 5.3 and -73.9 <= lon <= -34.8:
        return {
            "country": "Brazil",
            "country_code": "BR",
            "region": "Brazil",
            "confidence": "HIGH",
            "reasoning": "Coordinates within Brazil bounding box (33.8°S-5.3°N, 73.9-34.8°W)"
        }
    
    # Colombia
    if -4.2 <= lat <= 13.4 and -79.0 <= lon <= -66.9:
        return {
            "country": "Colombia",
            "country_code": "CO",
            "region": "Colombia",
            "confidence": "HIGH",
            "reasoning": "Coordinates within Colombia bounding box (4.2°S-13.4°N, 79.0-66.9°W)"
        }
    
    # Argentina
    if -55.1 <= lat <= -21.8 and -73.6 <= lon <= -53.6:
        return {
            "country": "Argentina",
            "country_code": "AR",
            "region": "Argentina",
            "confidence": "HIGH",
            "reasoning": "Coordinates within Argentina bounding box (21.8-55.1°S, 73.6-53.6°W)"
        }
    
    # General South America
    if -56.0 <= lat <= 13.0 and -82.0 <= lon <= -34.0:
        return {
            "country": "South America",
            "country_code": "SA",
            "region": "South America (general)",
            "confidence": "MEDIUM",
            "reasoning": "Coordinates within general South America bounding box"
        }
    
    # ========== AFRICA ==========
    
    # South Africa
    if -35.0 <= lat <= -22.1 and 16.5 <= lon <= 33.0:
        return {
            "country": "South Africa",
            "country_code": "ZA",
            "region": "South Africa",
            "confidence": "HIGH",
            "reasoning": "Coordinates within South Africa bounding box (22.1-35.0°S, 16.5-33.0°E)"
        }
    
    # Nigeria
    if 4.3 <= lat <= 13.9 and 2.7 <= lon <= 14.7:
        return {
            "country": "Nigeria",
            "country_code": "NG",
            "region": "Nigeria",
            "confidence": "HIGH",
            "reasoning": "Coordinates within Nigeria bounding box (4.3-13.9°N, 2.7-14.7°E)"
        }
    
    # General Africa
    if -35.0 <= lat <= 38.0 and -18.0 <= lon <= 52.0:
        return {
            "country": "Africa",
            "country_code": "AF",
            "region": "Africa (general)",
            "confidence": "MEDIUM",
            "reasoning": "Coordinates within general Africa bounding box"
        }
    
    # ========== CARIBBEAN ==========
    
    # Jamaica
    if 17.7 <= lat <= 18.5 and -78.4 <= lon <= -76.2:
        return {
            "country": "Jamaica",
            "country_code": "JM",
            "region": "Jamaica",
            "confidence": "HIGH",
            "reasoning": "Coordinates within Jamaica bounding box (17.7-18.5°N, 78.4-76.2°W)"
        }
    
    # Dominican Republic
    if 17.5 <= lat <= 19.9 and -72.0 <= lon <= -68.3:
        return {
            "country": "Dominican Republic",
            "country_code": "DO",
            "region": "Dominican Republic",
            "confidence": "HIGH",
            "reasoning": "Coordinates within Dominican Republic bounding box (17.5-19.9°N, 72.0-68.3°W)"
        }
    
    # General Caribbean
    if 10.0 <= lat <= 27.0 and -90.0 <= lon <= -59.0:
        return {
            "country": "Caribbean",
            "country_code": "CB",
            "region": "Caribbean (general)",
            "confidence": "MEDIUM",
            "reasoning": "Coordinates within general Caribbean bounding box"
        }
    
    # ========== CENTRAL AMERICA ==========
    if 7.0 <= lat <= 18.5 and -93.0 <= lon <= -77.0:
        return {
            "country": "Central America",
            "country_code": "CA",
            "region": "Central America",
            "confidence": "MEDIUM",
            "reasoning": "Coordinates within Central America bounding box (7.0-18.5°N, 93.0-77.0°W)"
        }
    
    # ========== INVALID / UNKNOWN ==========
    
    # Null Island (0,0)
    if abs(lat) < 0.1 and abs(lon) < 0.1:
        return {
            "country": "Unknown",
            "country_code": "XX",
            "region": "Null Island",
            "confidence": "NONE",
            "reasoning": "Coordinates at or near (0,0) - likely invalid/missing geocoding data"
        }
    
    # Antarctica
    if lat < -60:
        return {
            "country": "Antarctica",
            "country_code": "AQ",
            "region": "Antarctica",
            "confidence": "HIGH",
            "reasoning": "Coordinates south of 60°S latitude"
        }
    
    # Default unknown
    return {
        "country": "Unknown",
        "country_code": "XX",
        "region": "Unknown",
        "confidence": "NONE",
        "reasoning": f"Coordinates ({lat:.4f}, {lon:.4f}) do not match any known region bounding box"
    }


def main():
    print("=" * 70)
    print("Option C: Fast Country/Region Estimation")
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
                    records.append({
                        'id': row['id'],
                        'lat': 0.0,
                        'lon': 0.0
                    })
    
    print(f"Loaded {len(records):,} records")
    print()
    
    # Process all records
    output_file = 'output/unmatched_addresses_estimated.csv'
    
    # Track stats for methodology doc
    country_counts = {}
    confidence_counts = {'HIGH': 0, 'MEDIUM': 0, 'LOW': 0, 'NONE': 0}
    sample_reasonings = []
    
    with open(output_file, 'w', newline='') as f:
        fieldnames = ['id', 'latitude', 'longitude', 'country', 'country_code', 'region', 'confidence', 'reasoning']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        for i, record in enumerate(records, 1):
            result = get_location_estimate(record['lat'], record['lon'])
            
            writer.writerow({
                'id': record['id'],
                'latitude': record['lat'],
                'longitude': record['lon'],
                'country': result['country'],
                'country_code': result['country_code'],
                'region': result['region'],
                'confidence': result['confidence'],
                'reasoning': result['reasoning']
            })
            
            # Track stats
            country = result['country']
            country_counts[country] = country_counts.get(country, 0) + 1
            confidence_counts[result['confidence']] += 1
            
            # Collect sample reasonings
            if len(sample_reasonings) < 20 or (i % 5000 == 0 and len(sample_reasonings) < 50):
                sample_reasonings.append({
                    'id': record['id'],
                    'lat': record['lat'],
                    'lon': record['lon'],
                    'result': result
                })
            
            if i % 10000 == 0:
                print(f"Processed {i:,}/{len(records):,}...")
    
    print(f"\n✅ Output saved to: {output_file}")
    
    # Generate methodology document
    methodology_file = 'output/unmatched_addresses_methodology.md'
    
    with open(methodology_file, 'w') as f:
        f.write("# Unmatched Addresses - Location Estimation Methodology\n\n")
        f.write(f"**Generated:** December 9, 2025\n")
        f.write(f"**Total Records:** {len(records):,}\n\n")
        
        f.write("## Overview\n\n")
        f.write("These addresses could not be matched to US Census Tracts because they are located ")
        f.write("outside the United States or its territories. We estimated their locations using ")
        f.write("**geographic bounding boxes** - rectangular regions defined by latitude/longitude ")
        f.write("ranges that correspond to known countries and regions.\n\n")
        
        f.write("## Methodology\n\n")
        f.write("### How It Works\n\n")
        f.write("1. **No API calls** - This is a pure coordinate-based estimation\n")
        f.write("2. Each coordinate pair (latitude, longitude) is tested against predefined bounding boxes\n")
        f.write("3. Bounding boxes are checked in order from most specific to most general\n")
        f.write("4. The first matching bounding box determines the location estimate\n\n")
        
        f.write("### Bounding Box Hierarchy\n\n")
        f.write("We check regions in this order to maximize accuracy:\n\n")
        f.write("1. **US Territories** (Puerto Rico, Virgin Islands, Guam, etc.)\n")
        f.write("2. **Canadian Provinces** (Ontario, Quebec, BC, Alberta, etc.)\n")
        f.write("3. **Mexican States** (Baja California, Nuevo León, Tamaulipas, etc.)\n")
        f.write("4. **European Countries** (UK, Germany, France, Spain, Italy, etc.)\n")
        f.write("5. **Asian Countries** (India, UAE, Philippines, Japan, China, etc.)\n")
        f.write("6. **Australia/New Zealand**\n")
        f.write("7. **South American Countries**\n")
        f.write("8. **African Countries**\n")
        f.write("9. **Caribbean Islands**\n")
        f.write("10. **General Continental Regions** (fallback)\n\n")
        
        f.write("### Confidence Levels\n\n")
        f.write("| Level | Meaning |\n")
        f.write("|-------|--------|\n")
        f.write("| **HIGH** | Coordinates fall within a well-defined country/region bounding box |\n")
        f.write("| **MEDIUM** | Coordinates fall within a general continental bounding box |\n")
        f.write("| **LOW** | Coordinates are in a broad region with less certainty |\n")
        f.write("| **NONE** | Coordinates are invalid (0,0) or don't match any known region |\n\n")
        
        f.write("## Results Summary\n\n")
        f.write("### By Country/Region\n\n")
        f.write("| Country/Region | Count | % |\n")
        f.write("|---------------|------:|---:|\n")
        
        for country, count in sorted(country_counts.items(), key=lambda x: -x[1]):
            pct = count / len(records) * 100
            f.write(f"| {country} | {count:,} | {pct:.1f}% |\n")
        
        f.write("\n### By Confidence Level\n\n")
        f.write("| Confidence | Count | % |\n")
        f.write("|-----------|------:|---:|\n")
        for conf, count in sorted(confidence_counts.items(), key=lambda x: -x[1]):
            pct = count / len(records) * 100
            f.write(f"| {conf} | {count:,} | {pct:.1f}% |\n")
        
        f.write("\n## Sample Estimations\n\n")
        f.write("Here are examples showing how the estimation works:\n\n")
        
        for sample in sample_reasonings[:15]:
            f.write(f"### ID: {sample['id']}\n")
            f.write(f"- **Coordinates:** ({sample['lat']:.4f}, {sample['lon']:.4f})\n")
            f.write(f"- **Estimated Location:** {sample['result']['region']}, {sample['result']['country']}\n")
            f.write(f"- **Confidence:** {sample['result']['confidence']}\n")
            f.write(f"- **Reasoning:** {sample['result']['reasoning']}\n\n")
        
        f.write("## Limitations\n\n")
        f.write("1. **Bounding box overlap:** Some coordinates near borders may be assigned to the wrong country\n")
        f.write("2. **Ocean/water:** Coordinates over water may be assigned to nearby land regions\n")
        f.write("3. **Small territories:** Some small island nations may not have specific bounding boxes\n")
        f.write("4. **No street-level accuracy:** This only identifies country/region, not specific addresses\n\n")
        
        f.write("## Why These Addresses Don't Have Census Tracts\n\n")
        f.write("US Census Tracts are geographic subdivisions used by the Census Bureau to collect ")
        f.write("and report demographic data. They only exist within:\n\n")
        f.write("- The 50 US states\n")
        f.write("- Washington, DC\n")
        f.write("- Puerto Rico\n")
        f.write("- US Virgin Islands\n")
        f.write("- Guam\n")
        f.write("- American Samoa\n")
        f.write("- Northern Mariana Islands\n\n")
        f.write("Addresses in Canada, Mexico, Europe, or any other country will never have a US Census Tract.\n")
    
    print(f"✅ Methodology saved to: {methodology_file}")
    print()
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Total records: {len(records):,}")
    print()
    print("Top locations:")
    for country, count in sorted(country_counts.items(), key=lambda x: -x[1])[:10]:
        pct = count / len(records) * 100
        print(f"  {country}: {count:,} ({pct:.1f}%)")


if __name__ == "__main__":
    main()
