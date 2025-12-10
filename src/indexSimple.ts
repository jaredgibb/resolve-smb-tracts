/**
 * Simple single-threaded census tract geocoding pipeline
 * Uses streaming to process 19.4M addresses efficiently
 */

import * as fs from 'fs';
import * as path from 'path';
import { parse } from 'csv-parse';
import { createReadStream, createWriteStream } from 'fs';
import { loadCensusTractShapefiles, findTractForPoint, SpatialIndex } from './spatialIndex.js';

const OUTPUT_DIR = './output';
const LOGS_DIR = './logs';
const ROWS_PER_FILE = 500000; // Max rows per output file
const PROGRESS_INTERVAL = 50000; // Log progress every 50k addresses

interface Stats {
  totalProcessed: number;
  totalMatched: number;
  totalUnmatched: number;
  totalErrors: number;
  startTime: number;
}

const stats: Stats = {
  totalProcessed: 0,
  totalMatched: 0,
  totalUnmatched: 0,
  totalErrors: 0,
  startTime: Date.now(),
};

let logStream: fs.WriteStream;
let outputFile: fs.WriteStream | null = null;
let outputRowCount = 0;
let currentOutputPart = 1;
let unmatchedStream: fs.WriteStream;

function log(message: string) {
  const timestamp = new Date().toISOString();
  const logMessage = `[${timestamp}] ${message}`;
  console.log(logMessage);
  logStream.write(logMessage + '\n');
}

function ensureDir(dir: string): void {
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
}

function openNewOutputFile(): void {
  if (outputFile) {
    outputFile.end();
  }
  const filename = `tracts_part_${String(currentOutputPart).padStart(3, '0')}.csv`;
  const filePath = path.join(OUTPUT_DIR, filename);
  outputFile = createWriteStream(filePath);
  outputFile.write('id,census_tract_geoid\n');
  outputRowCount = 0;
  log(`📄 Started new output file: ${filename}`);
  currentOutputPart++;
}

function writeResult(addressId: string | number, geoid: string | null): void {
  if (!outputFile || outputRowCount >= ROWS_PER_FILE) {
    openNewOutputFile();
  }
  
  if (geoid) {
    outputFile!.write(`${addressId},${geoid}\n`);
    stats.totalMatched++;
  } else {
    unmatchedStream.write(`${addressId}\n`);
    stats.totalUnmatched++;
  }
  
  outputRowCount++;
  stats.totalProcessed++;
  
  // Progress logging
  if (stats.totalProcessed % PROGRESS_INTERVAL === 0) {
    const elapsed = (Date.now() - stats.startTime) / 1000;
    const rate = Math.round(stats.totalProcessed / elapsed);
    const matchRate = ((stats.totalMatched / stats.totalProcessed) * 100).toFixed(1);
    log(`📊 Progress: ${stats.totalProcessed.toLocaleString()} processed | ${rate.toLocaleString()}/sec | ${matchRate}% matched`);
  }
}

async function processAddressFile(
  filePath: string,
  spatialIndex: SpatialIndex
): Promise<number> {
  return new Promise((resolve, reject) => {
    const filename = path.basename(filePath);
    log(`📁 Processing: ${filename}`);
    
    const startCount = stats.totalProcessed;
    const fileStartTime = Date.now();
    
    const parser = createReadStream(filePath).pipe(
      parse({
        columns: true,
        skip_empty_lines: true,
        trim: true,
      })
    );

    parser.on('data', (record: any) => {
      try {
        const addressId = record.id;  // Column is 'id' not 'address_id'
        const lat = parseFloat(record.latitude);
        const lon = parseFloat(record.longitude);
        
        if (isNaN(lat) || isNaN(lon)) {
          writeResult(addressId, null);
          stats.totalErrors++;
          return;
        }
        
        const geoid = findTractForPoint(lat, lon, spatialIndex);
        writeResult(addressId, geoid);
      } catch (error) {
        stats.totalErrors++;
      }
    });

    parser.on('end', () => {
      const fileCount = stats.totalProcessed - startCount;
      const fileTime = ((Date.now() - fileStartTime) / 1000).toFixed(1);
      log(`   ✓ ${filename}: ${fileCount.toLocaleString()} records in ${fileTime}s`);
      resolve(fileCount);
    });

    parser.on('error', (error) => {
      log(`   ✗ Error processing ${filename}: ${error.message}`);
      reject(error);
    });
  });
}

async function main(): Promise<void> {
  // Setup directories
  ensureDir(OUTPUT_DIR);
  ensureDir(LOGS_DIR);
  
  // Setup logging
  logStream = createWriteStream(path.join(LOGS_DIR, 'geocoding.log'), { flags: 'a' });
  unmatchedStream = createWriteStream(path.join(OUTPUT_DIR, 'unmatched_addresses.csv'));
  unmatchedStream.write('address_id\n');

  log('========================================');
  log('Census Tract Geocoding Pipeline (Simple)');
  log('========================================');
  log(`Output directory: ${OUTPUT_DIR}`);
  log(`Max rows per output file: ${ROWS_PER_FILE}`);
  log('');

  try {
    // Load census tract shapefiles
    log('Loading census tract shapefiles...');
    const spatialIndex = await loadCensusTractShapefiles();
    log(`✓ Loaded ${spatialIndex.tracts.length} census tracts`);
    
    // Force garbage collection after loading
    if (global.gc) {
      global.gc();
      log('✓ Garbage collected after loading shapefiles');
    }

    // Find all address CSV files - process part files first (001-035), then gaps
    const addressFiles: string[] = [];
    const allFiles = fs.readdirSync('.');
    
    // Get part files and sort them numerically
    const partFiles = allFiles
      .filter(f => f.match(/^addresses_part_\d+\.csv$/))
      .sort((a, b) => {
        const numA = parseInt(a.match(/\d+/)?.[0] || '0');
        const numB = parseInt(b.match(/\d+/)?.[0] || '0');
        return numA - numB;
      });
    
    // Add part files first
    for (const file of partFiles) {
      addressFiles.push(path.resolve('.', file));
    }
    
    // Then add gaps file if it exists
    if (allFiles.includes('addresses_gaps.csv')) {
      addressFiles.push(path.resolve('.', 'addresses_gaps.csv'));
    }

    if (addressFiles.length === 0) {
      throw new Error('No address files found matching pattern addresses_part_*.csv or addresses_gaps.csv');
    }

    log(`Found ${addressFiles.length} address files to process`);

    // Process each file sequentially
    for (const filePath of addressFiles) {
      await processAddressFile(filePath, spatialIndex);
    }

    // Close output files
    if (outputFile) {
      outputFile.end();
    }
    unmatchedStream.end();

    // Final statistics
    const totalTime = ((Date.now() - stats.startTime) / 1000).toFixed(1);
    const avgRate = Math.round(stats.totalProcessed / (Date.now() - stats.startTime) * 1000);
    
    log('');
    log('========================================');
    log('GEOCODING COMPLETE');
    log('========================================');
    log(`Total processed: ${stats.totalProcessed.toLocaleString()}`);
    log(`  ✓ Matched: ${stats.totalMatched.toLocaleString()} (${((stats.totalMatched / stats.totalProcessed) * 100).toFixed(2)}%)`);
    log(`  ✗ Unmatched: ${stats.totalUnmatched.toLocaleString()}`);
    log(`  ⚠ Errors: ${stats.totalErrors.toLocaleString()}`);
    log(`Total time: ${totalTime}s`);
    log(`Average rate: ${avgRate.toLocaleString()}/sec`);
    log(`Output files: ${currentOutputPart - 1}`);

    logStream.end();
    process.exit(0);
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    log(`✗ Fatal error: ${errorMessage}`);
    logStream.end();
    process.exit(1);
  }
}

main();
