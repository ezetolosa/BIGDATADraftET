import os
import logging
import sqlite3
import pandas as pd
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_data_pipeline():
    """Test the complete data processing pipeline"""
    tests_passed = []
    
    # Test 1: Check if raw data exists
    logger.info("\nTesting raw data availability...")
    if os.path.exists('data/raw/database.sqlite'):
        tests_passed.append("Raw SQLite database found")
    else:
        logger.error("SQLite database not found in data/raw/")
        return False

    # Test 2: Test SQLite connection
    logger.info("\nTesting SQLite connection...")
    try:
        conn = sqlite3.connect('data/raw/database.sqlite')
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM Match")
        match_count = cursor.fetchone()[0]
        tests_passed.append(f"SQLite connection successful - {match_count} matches found")
    except Exception as e:
        logger.error(f"SQLite connection failed: {str(e)}")
        return False
    finally:
        if 'conn' in locals():
            conn.close()

    # Test 3: Test Spark setup
    logger.info("\nTesting Spark configuration...")
    try:
        spark = SparkSession.builder \
            .appName("Test Processing") \
            .config("spark.driver.memory", "1g") \
            .master("local[*]") \
            .getOrCreate()
        tests_passed.append("Spark session created successfully")
    except Exception as e:
        logger.error(f"Spark setup failed: {str(e)}")
        return False

    # Test 4: Check Parquet output
    logger.info("\nTesting Parquet data...")
    try:
        parquet_df = spark.read.parquet("output/predictions")
        row_count = parquet_df.count()
        col_count = len(parquet_df.columns)
        tests_passed.append(f"Parquet data loaded - {row_count} rows, {col_count} columns")
        
        # Validate required columns
        required_cols = [
            'date', 'home_team_long_name', 'away_team_long_name',
            'home_team_goal', 'away_team_goal', 'match_outcome',
            'league_name'
        ]
        missing_cols = [col for col in required_cols if col not in parquet_df.columns]
        
        if not missing_cols:
            tests_passed.append("All required columns present")
        else:
            logger.error(f"Missing columns: {missing_cols}")
            return False
            
    except Exception as e:
        logger.error(f"Parquet data test failed: {str(e)}")
        return False
    finally:
        if 'spark' in locals():
            spark.stop()

    # Display results
    logger.info("\nTest Results:")
    for result in tests_passed:
        logger.info(f"✓ {result}")
    
    return True

if __name__ == "__main__":
    success = test_processing.py()
    if success:
        logger.info("\nAll processing tests passed successfully")
    else:
        logger.error("\nProcessing tests failed - check logs for details")