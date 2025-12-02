#!/bin/bash
set -euo pipefail

echo "Extracting data..."
outdir="/Users/tanmaie/Desktop/magnoos/exercises/04_ETL/etl_cron_data/etl_cron_data"
data="/Users/tanmaie/Desktop/magnoos/exercises/01_pandas/bigmart_data.csv"
cleaned="$outdir/cleaned.csv"

mkdir -p "$outdir"
LOG="$outdir/etl.log"

echo "Transforming data..."
# drop any line that has empty fields ("", blank cells)
grep -v ',,' "$data" | grep -v ',$' | grep -v '^,' | grep -v '^$' > "$cleaned"


echo "Loading cleaned file..."
echo "ETL completed!"
echo "$(date '+%Y-%m-%d %H:%M:%S') - ETL completed" >> "$LOG"

# A cron job - tells your computer to run the script at certain time - either once or it can also keep going on and on
#27 11 * * * file path             11:27 crontab -e(Command)