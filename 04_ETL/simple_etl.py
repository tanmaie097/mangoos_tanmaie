import csv
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq

#csv, parquet paths 
raw_csv = Path("/Users/tanmaie/Desktop/magnoos/exercises/01_pandas/bigmart_data.csv")
out_path = Path("/Users/tanmaie/Desktop/magnoos/exercises/04_ETL/cleaned.parquet")
def extract_csv(csv_path):
    with csv_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)

def transform(rows):
    """
  cleaning data
  dropping rows with null
    """
    cleaned = []
    for row in rows:
        if all(row[col].strip() != "" for col in row):
            cleaned.append(row)

    return cleaned

def load(cleaned, out_path):
    table = pa.Table.from_pylist(cleaned)
    pq.write_table(table, out_path)


if __name__ == "__main__":
    print("Extracting...")
    raw_rows = extract_csv(raw_csv)

    print("Transforming...")
    cleaned_rows = transform(raw_rows)

    print("Loading cleaned CSV...")
    load(cleaned_rows, out_path)

    print("ETL complete! → cleaned_data.csv created.")