import * as fs from 'fs';
import * as path from 'path';
import { parse } from 'csv-parse';
import { loadCensusTractShapefiles, findTractForPoint } from './spatialIndex.js';

interface AddressRecord {
  id: string;
  lat: string;
  long: string;
  census_tract_geoid: string;
}

async function main() {
  const inputFile = 'check_these_again.csv';
  const outputFile = 'output/check_these_again_results.csv';
  
  console.log('========================================');
  console.log('Processing: check_these_again.csv');
  console.log('========================================');
  console.log();
  
  // Load census tract shapefiles
  console.log('Loading census tract shapefiles...');
  const spatialIndex = await loadCensusTractShapefiles();
  console.log(`✓ Loaded ${spatialIndex.tracts.length} census tracts`);
  console.log();
  
  // Create output stream
  const outputStream = fs.createWriteStream(outputFile);
  outputStream.write('id,census_tract_geoid\n');
  
  // Process input file
  const parser = fs.createReadStream(inputFile).pipe(
    parse({
      columns: true,
      skip_empty_lines: true,
      trim: true,
    })
  );
  
  let processed = 0;
  let matched = 0;
  let unmatched = 0;
  const startTime = Date.now();
  
  for await (const record of parser as AsyncIterable<AddressRecord>) {
    const id = record.id;
    const lat = parseFloat(record.lat);
    const lon = parseFloat(record.long);
    
    let tractGeoid = '';
    
    if (!isNaN(lat) && !isNaN(lon) && lat !== 0 && lon !== 0) {
      const tract = findTractForPoint(lat, lon, spatialIndex);
      if (tract) {
        tractGeoid = tract;
        matched++;
      } else {
        unmatched++;
      }
    } else {
      unmatched++;
    }
    
    // Write row (blank census_tract_geoid if no match)
    outputStream.write(`${id},${tractGeoid}\n`);
    
    processed++;
    
    // Progress every 50k
    if (processed % 50000 === 0) {
      const elapsed = (Date.now() - startTime) / 1000;
      const rate = Math.round(processed / elapsed);
      const matchRate = ((matched / processed) * 100).toFixed(1);
      console.log(`📊 Progress: ${processed.toLocaleString()} processed | ${rate}/sec | ${matchRate}% matched`);
    }
  }
  
  outputStream.end();
  
  const elapsed = (Date.now() - startTime) / 1000;
  
  console.log();
  console.log('========================================');
  console.log('COMPLETE');
  console.log('========================================');
  console.log(`Total processed: ${processed.toLocaleString()}`);
  console.log(`✅ Matched: ${matched.toLocaleString()} (${((matched/processed)*100).toFixed(2)}%)`);
  console.log(`❌ Unmatched: ${unmatched.toLocaleString()} (${((unmatched/processed)*100).toFixed(2)}%)`);
  console.log(`Time: ${elapsed.toFixed(1)}s`);
  console.log(`Output: ${outputFile}`);
}

main().catch(console.error);
