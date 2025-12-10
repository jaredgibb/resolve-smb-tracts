import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { parse } from 'csv-parse';
import { createReadStream, createWriteStream } from 'fs';
import Piscina from 'piscina';
import { loadCensusTractShapefiles, SpatialIndex } from './spatialIndex.js';

const OUTPUT_DIR = './output';
const LOGS_DIR = './logs';
const CHUNK_SIZE = 10000; // 10k rows per chunk
const ROWS_PER_FILE = 500000; // Max rows per output file
const WORKER_THREADS = Math.max(1, (typeof require !== 'undefined' && require('os').cpus().length - 1) || 4);

interface GeocodeResult {
  address_id: string | number;
  census_tract_geoid: string | null;
  error?: string;
}

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
let outputFilePath: string | null = null;
let outputRowCount = 0;
let currentOutputPart = 1;
let unmatchedStream: fs.WriteStream;

function log(message: string) {
  const timestamp = new Date().toISOString();
  const logMessage = `[${timestamp}] ${message}`;
  console.log(logMessage);
  logStream.write(logMessage + '\n');
}

function ensureDirectories() {
  if (!fs.existsSync(OUTPUT_DIR)) {
    fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  }
  if (!fs.existsSync(LOGS_DIR)) {
    fs.mkdirSync(LOGS_DIR, { recursive: true });
  }
}

function getOutputFilePath(partNumber: number): string {
  return path.join(OUTPUT_DIR, `tracts_part_${String(partNumber).padStart(3, '0')}.csv`);
}

function openNewOutputFile(): fs.WriteStream {
  // Close current file if exists
  if (outputFile) {
    outputFile.end();
  }

  outputFilePath = getOutputFilePath(currentOutputPart);
  outputFile = createWriteStream(outputFilePath, { flags: 'w' });

  // Write header
  outputFile.write('address_id,census_tract_geoid\n');

  outputRowCount = 0;
  log(`✓ Opened output file: ${outputFilePath}`);

  return outputFile;
}

async function writeResults(results: GeocodeResult[]) {
  if (!outputFile) {
    openNewOutputFile();
  }

  for (const result of results) {
    // Check if we need to rotate to new file
    if (outputRowCount >= ROWS_PER_FILE) {
      currentOutputPart++;
      openNewOutputFile();
    }

    // Write successful match
    if (result.census_tract_geoid) {
      outputFile!.write(`${result.address_id},${result.census_tract_geoid}\n`);
      stats.totalMatched++;
    } else {
      // Track unmatched
      stats.totalUnmatched++;
      if (result.error) {
        unmatchedStream.write(
          `${result.address_id},${result.error || 'no_match'}\n`
        );
      }
    }

    outputRowCount++;
    stats.totalProcessed++;
  }
}

async function processAddressFile(
  filePath: string,
  spatialIndex: SpatialIndex,
  pool: Piscina
): Promise<number> {
  return new Promise((resolve, reject) => {
    let chunkBuffer: Array<{
      address_id: string | number;
      latitude: number;
      longitude: number;
    }> = [];
    let processedCount = 0;

    const parser = createReadStream(filePath)
      .pipe(
        parse({
          columns: true,
          skip_empty_lines: true,
        })
      );

    parser.on('readable', async function () {
      let record;
      while ((record = parser.read()) !== null) {
        try {
          const lat = parseFloat(record.latitude);
          const lon = parseFloat(record.longitude);

          // Validate coordinates
          if (isNaN(lat) || isNaN(lon) || lat < -90 || lat > 90 || lon < -180 || lon > 180) {
            stats.totalErrors++;
            unmatchedStream.write(`${record.address_id},invalid_coordinates\n`);
            continue;
          }

          chunkBuffer.push({
            address_id: record.address_id,
            latitude: lat,
            longitude: lon,
          });

          // Process chunk when buffer reaches size
          if (chunkBuffer.length >= CHUNK_SIZE) {
            parser.pause();

            // Send to worker pool
            const chunk = chunkBuffer;
            chunkBuffer = [];

            try {
              const results = await pool.run({
                addresses: chunk,
                spatialIndex: {
                  items: spatialIndex.items,
                  tracts: spatialIndex.tracts,
                  bbox: spatialIndex.bbox,
                },
              });

              await writeResults(results);
              processedCount += chunk.length;
            } catch (error) {
              log(`✗ Error processing chunk: ${error}`);
            }

            parser.resume();
          }
        } catch (error) {
          log(`✗ Error parsing record: ${error}`);
          stats.totalErrors++;
        }
      }
    });

    parser.on('end', async () => {
      // Process remaining records in buffer
      if (chunkBuffer.length > 0) {
        try {
          const results = await pool.run({
            addresses: chunkBuffer,
            spatialIndex: {
              items: spatialIndex.items,
              tracts: spatialIndex.tracts,
              bbox: spatialIndex.bbox,
            },
          });

          await writeResults(results);
          processedCount += chunkBuffer.length;
        } catch (error) {
          log(`✗ Error processing final chunk: ${error}`);
        }
      }

      resolve(processedCount);
    });

    parser.on('error', reject);
  });
}

async function main() {
  ensureDirectories();
  logStream = createWriteStream(path.join(LOGS_DIR, 'geocoding.log'), { flags: 'a' });
  unmatchedStream = createWriteStream(path.join(OUTPUT_DIR, 'unmatched_addresses.csv'), {
    flags: 'w',
  });

  // Write header for unmatched
  unmatchedStream.write('address_id,error_reason\n');

  log('========================================');
  log('Census Tract Geocoding Pipeline');
  log('========================================');
  log(`Output directory: ${OUTPUT_DIR}`);
  log(`Worker threads: ${WORKER_THREADS}`);
  log(`Chunk size: ${CHUNK_SIZE} addresses`);
  log(`Max rows per output file: ${ROWS_PER_FILE}`);
  log('');

  try {
    // Load spatial index
    log('Loading census tract shapefiles...');
    const spatialIndex = await loadCensusTractShapefiles();
    log(`✓ Loaded ${spatialIndex.tracts.length} census tracts`);
    log('');

    // Initialize worker pool
    log(`Initializing worker pool with ${WORKER_THREADS} threads...`);
    const pool = new Piscina({
      filename: path.join(process.cwd(), 'dist/worker.js'),
      maxThreads: WORKER_THREADS,
    });

    // Discover address files
    log('Discovering address files...');
    const addressDir = '.';
    const addressFiles = fs
      .readdirSync(addressDir)
      .filter((f) => /^addresses_part_\d{3}\.csv$|^addresses_gaps\.csv$/.test(f))
      .sort();

    if (addressFiles.length === 0) {
      log('✗ No address files found matching pattern addresses_part_*.csv');
      process.exit(1);
    }

    log(`✓ Found ${addressFiles.length} address files`);
    log('');

    // Open first output file
    openNewOutputFile();

    // Process each address file
    let totalAddresses = 0;
    for (const addressFile of addressFiles) {
      log(`Processing ${addressFile}...`);
      const filePath = path.join(addressDir, addressFile);

      try {
        const count = await processAddressFile(filePath, spatialIndex, pool);
        totalAddresses += count;
        log(`  ✓ Processed ${count} addresses from ${addressFile}`);
      } catch (error) {
        log(`  ✗ Error processing ${addressFile}: ${error}`);
      }
    }

    log('');

    // Cleanup
    if (outputFile) {
      outputFile.end();
    }
    unmatchedStream.end();

    // Final stats
    const elapsedSeconds = (Date.now() - stats.startTime) / 1000;
    const matchRate = stats.totalProcessed > 0 ? (stats.totalMatched / stats.totalProcessed) * 100 : 0;

    log('========================================');
    log('Geocoding Complete');
    log('========================================');
    log(`✓ Total addresses processed: ${stats.totalProcessed}`);
    log(`✓ Successfully matched: ${stats.totalMatched}`);
    log(`✗ Unmatched: ${stats.totalUnmatched}`);
    log(`✗ Errors: ${stats.totalErrors}`);
    log(`Match rate: ${matchRate.toFixed(2)}%`);
    log(`Time elapsed: ${elapsedSeconds.toFixed(1)} seconds`);
    log(`Speed: ${(stats.totalProcessed / elapsedSeconds).toFixed(0)} addresses/sec`);
    log(`Output files: ${currentOutputPart} files in ${OUTPUT_DIR}`);
    log('========================================');

    logStream.end();
    process.exit(0);
  } catch (error) {
    log(`✗ Fatal error: ${error instanceof Error ? error.message : String(error)}`);
    logStream.end();
    process.exit(1);
  }
}

main();
