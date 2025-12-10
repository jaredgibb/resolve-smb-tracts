import * as fs from 'fs';
import * as path from 'path';
import RBush from 'rbush';
import { booleanPointInPolygon } from '@turf/boolean-point-in-polygon';
import { point } from '@turf/helpers';
import * as shapefile from 'shapefile';

export interface CensusTract {
  geoid: string;
  geometry: GeoJSONGeometry;
}

export interface GeoJSONGeometry {
  type: 'Polygon' | 'MultiPolygon';
  coordinates: any;
}

// RBush item interface
interface RBushItem {
  minX: number;
  minY: number;
  maxX: number;
  maxY: number;
  id: number;
}

// Export RBushItem for worker usage
export type { RBushItem };

export interface SpatialIndex {
  index: RBush<RBushItem>;
  tracts: CensusTract[];
  items: RBushItem[];  // For serialization to workers
  bbox: [number, number, number, number]; // [minX, minY, maxX, maxY]
}

const DATA_DIR = './data/census_tracts';

export async function loadCensusTractShapefiles(): Promise<SpatialIndex> {
  console.log('Loading census tract shapefiles...');

  const tracts: CensusTract[] = [];
  let minX = 180,
    minY = 90,
    maxX = -180,
    maxY = -90;

  try {
    const stateDir = fs.readdirSync(DATA_DIR, { withFileTypes: true });

    for (const dir of stateDir) {
      if (!dir.isDirectory() || !dir.name.startsWith('state_')) {
        continue;
      }

      const statePath = path.join(DATA_DIR, dir.name);
      const shpFile = fs
        .readdirSync(statePath)
        .find((f) => f.endsWith('.shp'));

      if (!shpFile) continue;

      const shpPath = path.join(statePath, shpFile);
      console.log(`  Loading ${dir.name}...`);

      try {
        const source = await shapefile.open(shpPath);
        let tractCount = 0;
        let geoidField: string | null = null;
        let feature;

        // Read features using async .read() method
        while ((feature = await source.read()) && !feature.done) {
          const f = feature.value;

          // Auto-detect GEOID field on first feature
          if (geoidField === null) {
            geoidField = Object.keys(f.properties).find((k) =>
              ['GEOID', 'GEOID20', 'geoid'].includes(k)
            ) || null;

            if (!geoidField) {
              console.warn(`    ⚠ No GEOID field found in ${dir.name}`);
              break;
            }
          }

          const geoid = String(f.properties[geoidField]);

          // Validate GEOID format (11 digits)
          if (!/^\d{11}$/.test(geoid)) {
            console.warn(`    ⚠ Invalid GEOID format: ${geoid}`);
            continue;
          }

          // Extract bounds for spatial index
          const coords = extractAllCoordinates(f.geometry);
          for (const [lon, lat] of coords) {
            minX = Math.min(minX, lon);
            minY = Math.min(minY, lat);
            maxX = Math.max(maxX, lon);
            maxY = Math.max(maxY, lat);
          }

          tracts.push({
            geoid,
            geometry: f.geometry as GeoJSONGeometry,
          });

          tractCount++;
        }

        console.log(`    ✓ Loaded ${tractCount} tracts from ${dir.name}`);
      } catch (error) {
        console.warn(
          `    ✗ Error loading ${shpPath}: ${error instanceof Error ? error.message : String(error)}`
        );
      }
    }
  } catch (error) {
    throw new Error(
      `Failed to load shapefiles: ${error instanceof Error ? error.message : String(error)}`
    );
  }

  if (tracts.length === 0) {
    throw new Error('No census tracts loaded. Run: npm run download-shapefiles');
  }

  console.log(`✓ Total tracts loaded: ${tracts.length}`);

  // Build spatial index using RBush
  console.log('Building spatial index...');
  const index = new RBush<RBushItem>();
  const items: RBushItem[] = [];

  for (let i = 0; i < tracts.length; i++) {
    const [tractMinX, tractMinY, tractMaxX, tractMaxY] = extractBounds(tracts[i].geometry);
    items.push({
      minX: tractMinX,
      minY: tractMinY,
      maxX: tractMaxX,
      maxY: tractMaxY,
      id: i,
    });
  }

  // Bulk load for better performance
  index.load(items);
  console.log('✓ Spatial index built');

  return {
    index,
    tracts,
    items,  // Include for worker serialization
    bbox: [minX, minY, maxX, maxY],
  };
}

function extractAllCoordinates(geometry: GeoJSONGeometry): Array<[number, number]> {
  const coords: Array<[number, number]> = [];

  function processCoordinates(coords: any): void {
    if (!coords) return;
    if (typeof coords[0] === 'number') {
      // [lon, lat]
      coords_push([coords[0], coords[1]]);
    } else if (Array.isArray(coords[0])) {
      for (const c of coords) {
        processCoordinates(c);
      }
    }
  }

  function coords_push(coord: [number, number]): void {
    coords.push(coord);
  }

  if (geometry.type === 'Polygon') {
    processCoordinates(geometry.coordinates);
  } else if (geometry.type === 'MultiPolygon') {
    for (const polygon of geometry.coordinates) {
      processCoordinates(polygon);
    }
  }

  return coords;
}

function extractBounds(geometry: GeoJSONGeometry): [number, number, number, number] {
  const coords = extractAllCoordinates(geometry);
  if (coords.length === 0) return [0, 0, 0, 0];

  let minX = coords[0][0],
    minY = coords[0][1],
    maxX = coords[0][0],
    maxY = coords[0][1];

  for (const [lon, lat] of coords) {
    minX = Math.min(minX, lon);
    minY = Math.min(minY, lat);
    maxX = Math.max(maxX, lon);
    maxY = Math.max(maxY, lat);
  }

  return [minX, minY, maxX, maxY];
}

export function findTractForPoint(
  lat: number,
  lon: number,
  spatialIndex: SpatialIndex
): string | null {
  // Validate coordinates
  if (lat < -90 || lat > 90 || lon < -180 || lon > 180) {
    return null;
  }

  const { index, tracts, bbox } = spatialIndex;

  // Quick bounds check
  if (lon < bbox[0] || lon > bbox[2] || lat < bbox[1] || lat > bbox[3]) {
    return null;
  }

  // Query spatial index for candidates (search with point as bbox)
  const candidates = index.search({
    minX: lon,
    minY: lat,
    maxX: lon,
    maxY: lat,
  });

  const testPoint = point([lon, lat]);

  // Test each candidate
  for (const item of candidates) {
    const tract = tracts[item.id];
    try {
      if (booleanPointInPolygon(testPoint, tract.geometry as any)) {
        return tract.geoid;
      }
    } catch (error) {
      // Skip invalid geometries
      console.warn(
        `  Warning: Invalid geometry for GEOID ${tract.geoid}: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }

  return null;
}
