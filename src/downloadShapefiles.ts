import * as fs from 'fs';
import * as path from 'path';
import * as https from 'https';
import * as http from 'http';
import { createWriteStream } from 'fs';
import { Extract } from 'unzipper';

const TIGER_BASE_URL = 'https://www2.census.gov/geo/tiger/TIGER2025/TRACT/';

// State FIPS codes (01-56, excluding territories)
const STATE_FIPS_CODES = [
  '01', '02', '04', '05', '06', '08', '09', '10', '12', '13',
  '15', '16', '17', '18', '19', '20', '21', '22', '23', '24',
  '25', '26', '27', '28', '29', '30', '31', '32', '33', '34',
  '35', '36', '37', '38', '39', '40', '41', '42', '44', '45',
  '46', '47', '48', '49', '50', '51', '53', '54', '55', '56'
];

const DATA_DIR = './data/census_tracts';
const LOG_FILE = './logs/download_shapefiles.log';

let logStream: fs.WriteStream;

function log(message: string) {
  const timestamp = new Date().toISOString();
  const logMessage = `${timestamp} - ${message}`;
  console.log(logMessage);
  logStream.write(logMessage + '\n');
}

function ensureDirectories() {
  if (!fs.existsSync(DATA_DIR)) {
    fs.mkdirSync(DATA_DIR, { recursive: true });
  }
  if (!fs.existsSync('./logs')) {
    fs.mkdirSync('./logs', { recursive: true });
  }
}

function downloadFile(url: string, filename: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const protocol = url.startsWith('https') ? https : http;
    const file = fs.createWriteStream(filename);

    protocol
      .get(url, (response) => {
        if (response.statusCode === 404) {
          file.destroy();
          reject(new Error(`File not found: ${url}`));
          return;
        }
        response.pipe(file);
        file.on('finish', () => {
          file.close();
          resolve();
        });
      })
      .on('error', (err) => {
        fs.unlink(filename, () => {}); // Delete partial file
        reject(err);
      });
  });
}

function extractZip(zipPath: string, extractPath: string): Promise<void> {
  return new Promise((resolve, reject) => {
    fs.createReadStream(zipPath)
      .pipe(Extract({ path: extractPath }))
      .on('close', resolve)
      .on('error', reject);
  });
}

async function downloadStateShapefiles() {
  log('========================================');
  log('Starting TIGER/Line Shapefile Download');
  log('========================================');
  log(`Source: ${TIGER_BASE_URL}`);
  log(`Download to: ${DATA_DIR}`);
  log('');

  let successCount = 0;
  let failureCount = 0;
  const failedStates: string[] = [];

  for (const fips of STATE_FIPS_CODES) {
    try {
      const filename = `tl_2025_${fips}_tract.zip`;
      const url = TIGER_BASE_URL + filename;
      const zipPath = path.join(DATA_DIR, filename);
      const extractPath = path.join(DATA_DIR, `state_${fips}`);

      // Skip if already downloaded AND has .shp file
      if (fs.existsSync(extractPath)) {
        const files = fs.readdirSync(extractPath);
        const hasShp = files.some(f => f.endsWith('.shp'));
        if (hasShp) {
          log(`✓ State ${fips} already downloaded, skipping`);
          successCount++;
          continue;
        } else {
          // Remove incomplete directory
          fs.rmSync(extractPath, { recursive: true, force: true });
          log(`  Removing incomplete state_${fips} directory`);
        }
      }

      // Remove any leftover zip files
      if (fs.existsSync(zipPath)) {
        fs.unlinkSync(zipPath);
      }

      log(`⬇ Downloading state ${fips} (${filename})...`);
      await downloadFile(url, zipPath);
      log(`  ✓ Downloaded, extracting...`);

      if (!fs.existsSync(extractPath)) {
        fs.mkdirSync(extractPath, { recursive: true });
      }

      await extractZip(zipPath, extractPath);
      log(`  ✓ Extracted to ${extractPath}`);

      // Delete zip after extraction
      fs.unlinkSync(zipPath);
      log(`  ✓ Cleaned up zip file`);

      successCount++;
    } catch (error) {
      const err = error instanceof Error ? error.message : String(error);
      log(`✗ State ${fips} failed: ${err}`);
      failureCount++;
      failedStates.push(fips);
    }
  }

  log('');
  log('========================================');
  log('Download Summary');
  log('========================================');
  log(`✓ Successful: ${successCount} states`);
  log(`✗ Failed: ${failureCount} states`);
  if (failedStates.length > 0) {
    log(`Failed states: ${failedStates.join(', ')}`);
  }
  log('========================================');
}

async function main() {
  ensureDirectories();
  logStream = fs.createWriteStream(LOG_FILE, { flags: 'a' });

  try {
    await downloadStateShapefiles();
    logStream.end();
    process.exit(0);
  } catch (error) {
    log(`Fatal error: ${error instanceof Error ? error.message : String(error)}`);
    logStream.end();
    process.exit(1);
  }
}

main();
