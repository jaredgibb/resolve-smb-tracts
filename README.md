# Address Data Fetcher

Fetches all address data from Supabase API and saves to CSV.

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

Run the script:
```bash
python fetch_addresses.py
```

The script will:
- Fetch data in batches of 1,000 rows
- Display progress for each batch
- Save all data to `addresses.csv`
- Handle interruptions gracefully (Ctrl+C will save partial data)

## Features

- **Automatic pagination**: Continues until all ~20 million rows are fetched
- **Progress tracking**: Shows batch number, offset, rows fetched, and timing
- **Error handling**: Graceful handling of network errors and interruptions
- **Resumable**: Can be modified to resume from last offset if interrupted

## Output

- File: `addresses.csv`
- Format: CSV with headers from the API response
- Encoding: UTF-8
