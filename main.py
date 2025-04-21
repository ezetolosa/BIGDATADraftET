# main.py – Full pipeline from data collection to model training
import os
from pyspark.sql import SparkSession
from src.data_processing import DataProcessor
from src.feature_engineering import FeatureEngineer
from src.model_training import ModelTrainer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    return SparkSession.builder \
        .appName("SoccerAnalytics") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

def main():
    try:
        os.makedirs("data/processed", exist_ok=True)
        os.makedirs("models", exist_ok=True)
        os.makedirs("output", exist_ok=True)

        spark = create_spark_session()

        processor = DataProcessor(spark)
        feature_engineer = FeatureEngineer(spark)
        model_trainer = ModelTrainer(spark)

        logger.info("Loading and cleaning match data...")
        match_data = processor.load_and_clean_match_data()

        logger.info("Generating features...")
        featured_data = feature_engineer.create_team_features(match_data)
        featured_data.write.mode("overwrite").parquet("data/processed/featured_matches.parquet")

        logger.info("Training match result classifier...")
        clf_model, test_data = model_trainer.train_result_classifier(featured_data)
        accuracy = model_trainer.evaluate_classifier(clf_model, test_data)
        logger.info(f"Classifier Accuracy: {accuracy:.2%}")
        model_trainer.save_model(clf_model, "models/result_classifier")

        logger.info("Training goal prediction regressor...")
        reg_model = model_trainer.train_goal_regressor(featured_data)
        model_trainer.save_model(reg_model, "models/goal_regressor")

        logger.info("Pipeline completed successfully.")

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    main()