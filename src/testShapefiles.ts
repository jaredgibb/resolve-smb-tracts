import * as fs from 'fs';
import * as path from 'path';
import * as shapefile from 'shapefile';

const DATA_DIR = './data/census_tracts';

async function testShapefiles() {
  console.log('========================================');
  console.log('Testing Shapefile Load');
  console.log('========================================');

  try {
    // Find first state directory
    const dirs = fs.readdirSync(DATA_DIR, { withFileTypes: true });
    const stateDir = dirs.find((d) => d.isDirectory() && d.name.startsWith('state_'));

    if (!stateDir) {
      console.log('✗ No state directories found in', DATA_DIR);
      console.log('  Run: npm run download-shapefiles');
      return;
    }

    const stateDataDir = path.join(DATA_DIR, stateDir.name);
    const shpFile = fs
      .readdirSync(stateDataDir)
      .find((f) => f.endsWith('.shp'));

    if (!shpFile) {
      console.log(`✗ No .shp file found in ${stateDataDir}`);
      return;
    }

    const shpPath = path.join(stateDataDir, shpFile);
    console.log(`✓ Found shapefile: ${shpPath}`);
    console.log('');

    // Test loading
    console.log('Loading shapefile...');
    const source = await shapefile.open(shpPath);
    console.log('✓ Shapefile opened successfully');

    // Read features using the .read() method
    console.log('Reading features...');
    let count = 0;
    let geoidField: string | null = null;
    let feature;

    // Read is async, need to await it
    while ((feature = await source.read()) && !feature.done) {
      const f = feature.value;

      if (count === 0) {
        // Detect GEOID field
        const props = f.properties;
        console.log(`  Available properties: ${Object.keys(props).join(', ')}`);

        geoidField = (Object.keys(props).find((k) =>
          ['GEOID', 'GEOID20', 'geoid'].includes(k)
        ) || null) as string | null;

        if (geoidField) {
          console.log(`  ✓ Found GEOID field: ${geoidField}`);
        } else {
          console.log(`  ✗ No GEOID field found`);
        }
      }

      if (count < 5) {
        const props = f.properties;
        console.log(`\n  Feature ${count + 1} (${f.geometry.type}):`);
        // Print all properties
        for (const [key, value] of Object.entries(props)) {
          console.log(`    ${key}: ${value}`);
        }
      }

      count++;
      if (count >= 5) break;
    }

    console.log('');
    console.log('✓ Test successful!');
    console.log(`  Total features read: ${count}`);
    console.log(`  GEOID field: ${geoidField || 'NOT FOUND'}`);
    console.log('========================================');
  } catch (error) {
    console.log('✗ Error:', error instanceof Error ? error.message : String(error));
  }
}

testShapefiles();
