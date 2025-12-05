import json
import csv
from datetime import datetime


RAW_FOLDER = "/Users/tanmaie/Desktop/magnoos/exercises/04_ETL/news_etl/raw"
OUTPUT_FOLDER = "/Users/tanmaie/Desktop/magnoos/exercises/04_ETL/news_etl/output"


today = datetime.now().strftime("%Y-%m-%d")

raw_file = f"{RAW_FOLDER}/raw_news_{today}.json"
csv_file = f"{OUTPUT_FOLDER}/news_data_{today}.csv"

print("Reading raw file:", raw_file)

#  Read raw JSON file
with open(raw_file, "r") as f:
    data = json.load(f)

articles = data.get("articles", [])

rows = []

# Transform & DROP NULL records
for article in articles:
    source = article.get("source", {}).get("name")
    author = article.get("author")
    title = article.get("title")
    url = article.get("url")
    published_at = article.get("publishedAt")

    # DROP if any are null
    if not source or not author or not title or not url or not published_at:
        continue  # skip this row

    rows.append([source, author, title, url, published_at])

print("Rows after dropping null:", len(rows))

# Save to CSV
with open(csv_file, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["source", "author", "title", "url", "published_at"]) # header

    for row in rows:
        writer.writerow(row)

print("CSV saved to:", csv_file)
