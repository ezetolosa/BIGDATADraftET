# extract_sqlite_to_csv.py – Extract tables from database.sqlite to CSV and Parquet
import sqlite3
import pandas as pd
from pyspark.sql import SparkSession
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_and_transform():
    """Extract data from SQLite and transform to CSV and Parquet"""
    try:
        # Create Spark session
        spark = SparkSession.builder \
            .appName("Extract SQLite") \
            .config("spark.driver.memory", "1g") \
            .master("local[*]") \
            .getOrCreate()

        # Connect to SQLite
        logger.info("Connecting to SQLite database...")
        conn = sqlite3.connect('data/raw/database.sqlite')
        
        # Extract match data with joins
        logger.info("Extracting match data...")
        matches = pd.read_sql_query("""
            SELECT 
                m.date,
                m.home_team_api_id,
                m.away_team_api_id,
                m.home_team_goal,
                m.away_team_goal,
                t.team_long_name AS home_team_name,
                t2.team_long_name AS away_team_name
            FROM Match m
            JOIN Team t ON m.home_team_api_id = t.team_api_id
            JOIN Team t2 ON m.away_team_api_id = t2.team_api_id
        """, conn)
        
        # Save to CSV
        output_path_csv = os.path.join("data/raw", "matches.csv")
        matches.to_csv(output_path_csv, index=False)
        logger.info(f"Extracted match data to {output_path_csv}")
        
        # Save to Parquet
        output_path_parquet = os.path.join("data/raw", "matches.parquet")
        spark_df = spark.createDataFrame(matches)
        spark_df.write.parquet(output_path_parquet)
        logger.info(f"Extracted match data to {output_path_parquet}")
        
    except Exception as e:
        logger.error(f"Error extracting match data: {e}")
    finally:
        conn.close()
        logger.info("Extraction complete!")

if __name__ == "__main__":
    extract_and_transform()