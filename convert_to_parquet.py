from pyspark.sql import SparkSession
import os
from pathlib import Path

def convert_sqlite_to_parquet():
    """Convert SQLite database to Parquet format"""
    try:
        # Get absolute path to JAR file
        jar_path = str(Path('lib/sqlite-jdbc-3.44.1.0.jar').absolute())
        
        # Create Spark session with SQLite JDBC driver
        spark = SparkSession.builder \
            .appName("SQLite to Parquet Converter") \
            .config("spark.driver.host", "localhost") \
            .config("spark.driver.bindAddress", "localhost") \
            .config("spark.driver.extraClassPath", jar_path) \
            .getOrCreate()

        print("Reading SQLite database...")
        df = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:sqlite:data/raw/database.sqlite") \
            .option("dbtable", "Match") \
            .option("driver", "org.sqlite.JDBC") \
            .load()

        print(f"Found {df.count()} rows")
        
        # Create output directory
        os.makedirs("data/raw", exist_ok=True)
        output_path = "data/raw/matches.parquet"

        # Write to Parquet
        print(f"Converting to Parquet format: {output_path}")
        df.write.mode("overwrite").parquet(output_path)
        
        print("✅ Successfully converted SQLite database to Parquet format")
        spark.stop()

    except Exception as e:
        print(f"❌ Error: {str(e)}")

if __name__ == "__main__":
    convert_sqlite_to_parquet()