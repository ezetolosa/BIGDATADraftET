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
    print(f"📤 Extracting: {table} → CSV")
    df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
    df.to_csv(f"{output_dir}/{table.lower()}.csv", index=False)

print("✅ All tables extracted to data/raw/")

# Close connection
conn.close()