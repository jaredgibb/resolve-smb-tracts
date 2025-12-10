import { findTractForPoint, SpatialIndex, CensusTract, RBushItem } from './spatialIndex.js';
import RBush from 'rbush';

interface WorkerInput {
  addresses: Array<{
    address_id: string | number;
    latitude: number;
    longitude: number;
  }>;
  spatialIndex: {
    items: RBushItem[];
    tracts: CensusTract[];
    bbox: [number, number, number, number];
  };
}

interface ProcessResult {
  address_id: string | number;
  census_tract_geoid: string | null;
  error?: string;
}

// Piscina uses default export function
export default function processAddresses(input: WorkerInput): ProcessResult[] {
  const { addresses, spatialIndex: indexData } = input;
  const results: ProcessResult[] = [];

  // Reconstruct the spatial index from items
  let spatialIndex: SpatialIndex;
  try {
    const index = new RBush<RBushItem>();
    index.load(indexData.items);
    spatialIndex = {
      index,
      tracts: indexData.tracts,
      items: indexData.items,
      bbox: indexData.bbox,
    };
  } catch (error) {
    // Return all as errors if index fails
    return addresses.map((addr) => ({
      address_id: addr.address_id,
      census_tract_geoid: null,
      error: `index_error: ${error instanceof Error ? error.message : String(error)}`,
    }));
  }

  for (const addr of addresses) {
    try {
      const lat = Number(addr.latitude);
      const lon = Number(addr.longitude);

      if (isNaN(lat) || isNaN(lon)) {
        results.push({
          address_id: addr.address_id,
          census_tract_geoid: null,
          error: 'invalid_coordinates',
        });
        continue;
      }

      const geoid = findTractForPoint(lat, lon, spatialIndex);

      results.push({
        address_id: addr.address_id,
        census_tract_geoid: geoid,
      });
    } catch (error) {
      results.push({
        address_id: addr.address_id,
        census_tract_geoid: null,
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }

  return results;
}
