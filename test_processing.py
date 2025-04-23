import os
import logging
import sqlite3
from pyspark.sql import SparkSession
import sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_data_pipeline():
    """Test the complete data processing pipeline"""
    tests_passed = []
    
    # Test 1: Check directory structure
    logger.info("\nTesting directory structure...")
    required_dirs = [
        'data/raw',
        'data/processed',
        'output/predictions',
        'output/plots',
        'output/plots/league_analysis',
        'analysis/predictions',
        'analysis/team',
        'analysis/league',
        'analysis/form'
    ]
    
    for dir_path in required_dirs:
        if os.path.exists(dir_path):
            tests_passed.append(f"Directory exists: {dir_path}")
        else:
            logger.error(f"Missing directory: {dir_path}")
            return False

    # Test 2: Check SQLite database
    logger.info("\nTesting SQLite database...")
    if os.path.exists('data/raw/database.sqlite'):
        try:
            conn = sqlite3.connect('data/raw/database.sqlite')
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM Match")
            match_count = cursor.fetchone()[0]
            tests_passed.append(f"SQLite database valid - {match_count} matches found")
            conn.close()
        except Exception as e:
            logger.error(f"SQLite database error: {str(e)}")
            return False
    else:
        logger.error("SQLite database not found")
        return False

    # Test 3: Check Parquet data
    logger.info("\nTesting Parquet data...")
    try:
        spark = SparkSession.builder \
            .appName("Test Processing") \
            .config("spark.driver.memory", "1g") \
            .master("local[*]") \
            .getOrCreate()
            
        if os.path.exists('output/predictions'):
            df = spark.read.parquet("output/predictions")
            row_count = df.count()
            col_count = len(df.columns)
            tests_passed.append(f"Parquet data valid - {row_count} rows, {col_count} columns")
            
            # Check required columns
            required_cols = [
                'date', 'home_team_long_name', 'away_team_long_name',
                'home_team_goal', 'away_team_goal', 'match_outcome',
                'league_name'
            ]
            missing_cols = [col for col in required_cols if col not in df.columns]
            
            if not missing_cols:
                tests_passed.append("All required columns present")
            else:
                logger.error(f"Missing columns: {missing_cols}")
                return False
        else:
            logger.error("Parquet data not found")
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
        logger.info(f"✅ {result}")
    
    return True

if __name__ == "__main__":
    success = test_data_pipeline()
    if success:
        logger.info("\n✅ All processing tests passed successfully")
        sys.exit(0)
    else:
        logger.error("\n❌ Processing tests failed - check logs for details")
        sys.exit(1)