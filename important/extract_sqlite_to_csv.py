# extract_sqlite_to_csv.py – Extract tables from database.sqlite to CSV
import sqlite3
import pandas as pd
import os

# Paths
db_path = "data/raw/database.sqlite"
output_dir = "data/raw"

# Tables you want to extract
tables_to_extract = ["Match", "Team", "League", "Player"]

# Ensure output directory exists
os.makedirs(output_dir, exist_ok=True)

# Connect to SQLite database
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Extract each table as CSV
for table in tables_to_extract:
    try:
        # Read table
        df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
        
        # Save to CSV
        output_path = os.path.join(output_dir, f"{table.lower()}.csv")
        df.to_csv(output_path, index=False)
        print(f"Extracted {table} to {output_path}")
        
    except Exception as e:
        print(f"Error extracting {table}: {e}")

conn.close()
print("Extraction complete!")