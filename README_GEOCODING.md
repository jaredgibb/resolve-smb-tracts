# Census Tract Geocoding Pipeline

This TypeScript/Node.js application geocodes addresses (latitude, longitude points) to US Census Bureau census tracts using official TIGER/Line 2025 shapefiles.

## Setup

1. Install dependencies:
```bash
npm install
```

2. Download census tract shapefiles from the US Census Bureau:
```bash
npm run download-shapefiles
```

This will download ~2GB of data to `./data/census_tracts/` automatically. You can also download manually:
- Source: https://www2.census.gov/geo/tiger/TIGER2025/TRACT/
- Extract to: `./data/census_tracts/`

3. Test the shapefiles:
```bash
npm run test-api
```

## Usage

### Geocode addresses:
```bash
npm run build
npm start
```

Or run directly with ts-node:
```bash
npm run dev
```

## Input Files

Address CSV files should match the pattern: `addresses_part_*.csv` or `addresses_gaps.csv`

Expected columns:
- `address_id` - Unique identifier
- `latitude` - WGS84 latitude (-90 to 90)
- `longitude` - WGS84 longitude (-180 to 180)

Example:
```csv
address_id,latitude,longitude,...
123,37.7749,-122.4194,...
456,34.0522,-118.2437,...
```

## Output

### Success: `output/tracts_part_*.csv`
Simple two-column format for database import:
```csv
address_id,census_tract_geoid
123,06001401100
456,06037504600
```

GEOID is 11-digit FIPS code: `[State FIPS (2)][County FIPS (3)][Tract (6)]`

### Errors: `output/unmatched_addresses.csv`
```csv
address_id,error_reason
789,invalid_coordinates
```

## Performance

- Uses KDBush spatial index for O(log n) bounding box queries
- Parallel processing with Turf.js point-in-polygon tests
- Streams 19M+ addresses efficiently with 10k row chunks
- Output split into 500k row chunks for database manageability

## Configuration

Edit `src/index.ts`:
- `CHUNK_SIZE` - Addresses per batch sent to workers (default: 10,000)
- `ROWS_PER_FILE` - Max rows per output CSV (default: 500,000)
- `WORKER_THREADS` - CPU cores - 1 (auto-detected)
