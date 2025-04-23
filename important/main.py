# main.py – Full pipeline from data collection to model training
import os
import shutil
import logging
import tempfile
from pathlib import Path
from pyspark.sql import SparkSession
from src.data_processing import DataProcessor
from src.feature_engineering import FeatureEngineer
from src.model_training import ModelTrainer
from src.utils.sqlite_to_csv import convert_database
import yaml
import subprocess

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def create_spark_session():
    return SparkSession.builder \
        .appName("Soccer Match Predictor") \
        .config("spark.sql.shuffle.partitions", "10") \
        .config("spark.default.parallelism", "4") \
        .config("spark.driver.memory", "4g") \
        .config("spark.memory.fraction", "0.6") \
        .config("spark.memory.storageFraction", "0.5") \
        .config("spark.sql.parquet.writeLegacyFormat", "true") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def ensure_data_files():
    required_files = ['match.csv', 'team.csv', 'league.csv', 'player.csv']
    missing_files = [f for f in required_files 
                    if not os.path.exists(f"data/raw/{f}")]
    
    if missing_files:
        logging.info("Extracting data from SQLite to CSV...")
        subprocess.run(["python", "extract_sqlite_to_csv.py"], check=True)

def ensure_directory(directory):
    """Safely create or clean a directory"""
    path = Path(directory)
    if path.exists():
        if path.is_file():
            path.unlink()
        else:
            # Remove contents but keep directory
            for item in path.glob('*'):
                if item.is_file():
                    item.unlink()
                elif item.is_dir():
                    shutil.rmtree(item, ignore_errors=True)
    else:
        path.mkdir(parents=True)

def cleanup_directories():
    """Remove existing output and model directories"""
    directories = ['output', 'models']
    for directory in directories:
        ensure_directory(directory)

def main():
    # Setup logging
    setup_logging()
    logger = logging.getLogger(__name__)

    # Load configuration
    with open('config/settings.yaml', 'r') as f:
        config = yaml.safe_load(f)

    logger.info("Starting data processing pipeline")

    try:
        os.makedirs("data/processed", exist_ok=True)
        os.makedirs("models", exist_ok=True)
        os.makedirs("output", exist_ok=True)

        # Ensure CSV files exist
        ensure_data_files()

        # Initialize Spark with optimized configuration
        spark = create_spark_session()

        # Clean up existing directories
        cleanup_directories()

        # Process data
        processor = DataProcessor(spark)
        match_data = processor.load_and_clean_match_data()
        
        logger.info("Generating features...")
        feature_engineer = FeatureEngineer(spark)
        featured_data = feature_engineer.create_team_features(match_data)
        
        # Train model
        logger.info("Training match result classifier...")
        model_trainer = ModelTrainer(spark)
        model, predictions = model_trainer.train_result_classifier(featured_data)
        
        # Repartition before saving to reduce memory pressure
        predictions = predictions.repartition(4)
        
        logger.info("Saving results...")
        model.write().overwrite().save("models/match_predictor")
        predictions.write.mode('overwrite').option("maxRecordsPerFile", "10000").parquet("output/predictions")
        
        logger.info("Pipeline completed successfully!")

    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        raise
    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    main()
