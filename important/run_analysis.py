# run_analysis.py – Interactive analysis tool
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg, round, count, desc, sum, col, 
    when, year, month, format_number
)
from src.model_training import ModelTrainer
from src.data_processing import DataProcessor
from src.feature_engineering import FeatureEngineer
from src.analysis.performance_metrics import TeamPerformanceAnalyzer
from src.visualization.plot_results import plot_team_performance
from src.utils.validation import setup_logging
import logging
import shutil
import tempfile
import os
import yaml
import matplotlib
matplotlib.use('Agg')  # Set non-interactive backend before importing pyplot
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.ml.classification import RandomForestClassificationModel
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create and return a Spark session"""
    return SparkSession.builder \
        .appName("Soccer Match Analysis") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

def cleanup_temp_dir(temp_dir):
    """Safely cleanup temporary directory"""
    try:
        # Wait for Spark to release files
        import time
        time.sleep(1)
        
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)
    except Exception as e:
        logger.warning(f"Cleanup warning (non-critical): {str(e)}")

def analyze_soccer_data():
    """Analyze soccer match data with enhanced metrics"""
    temp_dir = None
    try:
        # Initialize Spark
        spark, temp_dir = create_spark_session()
        
        # Read data
        df = spark.read.parquet("data/raw/matches.parquet")
        
        # 1. League Performance Analysis
        print("\n🏆 League Performance Analysis:")
        league_stats = df.groupBy("league_id").agg(
            count("*").alias("matches_played"),
            round(avg("home_team_goal"), 2).alias("avg_home_goals"),
            round(avg("away_team_goal"), 2).alias("avg_away_goals"),
            round(avg(col("home_team_goal") + col("away_team_goal")), 2).alias("avg_total_goals"),
            round(avg(when(col("home_team_goal") > col("away_team_goal"), 1)
                    .when(col("home_team_goal") < col("away_team_goal"), 0)
                    .otherwise(0.5)) * 100, 2).alias("home_win_pct")
        ).orderBy(desc("avg_total_goals"))
        
        league_stats.show()

        # 2. Seasonal Scoring Trends (Fixed)
        print("\n📈 Seasonal Goal Analysis:")
        seasonal = df.groupBy(year("date").alias("year")) \
            .agg(
                round(avg(col("home_team_goal") + col("away_team_goal")), 2).alias("avg_goals_per_match"),
                count("*").alias("matches_played")
            ).orderBy("year")
            
        seasonal.show()

        # 3. Most Exciting Matches
        print("\n⚽ Top 5 Highest Scoring Matches:")
        df.select(
            "date",
            "league_id",
            "home_team_goal",
            "away_team_goal",
            (col("home_team_goal") + col("away_team_goal")).alias("total_goals")
        ).orderBy(desc("total_goals")).limit(5).show()

        # Cleanup
        spark.stop()
        cleanup_temp_dir(temp_dir)
        
    except Exception as e:
        logger.error(f"❌ Error: {str(e)}")
        if temp_dir:
            cleanup_temp_dir(temp_dir)

def load_model_and_predictions():
    spark = SparkSession.builder \
        .appName("Soccer Match Analysis") \
        .getOrCreate()
    
    # Load the saved model
    model = RandomForestClassificationModel.load("models/match_predictor")
    
    # Load predictions
    predictions = spark.read.parquet("output/predictions")
    return spark, model, predictions

def analyze_predictions(spark):
    """Analyze prediction results"""
    try:
        # Read predictions from parquet
        predictions = spark.read.parquet("output/predictions")
        predictions.cache()
        
        # Calculate metrics
        total = predictions.count()
        correct = predictions.filter(col("prediction") == col("match_outcome")).count()
        accuracy = correct / total
        
        logger.info(f"\nTotal matches analyzed: {total}")
        logger.info(f"Correct predictions: {correct}")
        logger.info(f"Overall accuracy: {accuracy:.4f}")
        
        # Create confusion matrix
        confusion = predictions.groupBy("match_outcome").agg(
            count(when(col("prediction") == 0, 1)).alias("Predicted_Home_Win"),
            count(when(col("prediction") == 1, 1)).alias("Predicted_Draw"),
            count(when(col("prediction") == 2, 1)).alias("Predicted_Away_Win")
        ).toPandas()
        
        predictions.unpersist()
        return confusion
        
    except Exception as e:
        logger.error(f"Error analyzing predictions: {e}")
        raise

def plot_confusion_matrix(confusion_df):
    """Generate and save confusion matrix visualization"""
    try:
        plt.figure(figsize=(10, 8))
        sns.heatmap(
            confusion_df.iloc[:, 1:],
            annot=True,
            fmt='g',
            cmap='Blues',
            xticklabels=['Home Win', 'Draw', 'Away Win'],
            yticklabels=['Home Win', 'Draw', 'Away Win']
        )
        plt.title('Match Prediction Confusion Matrix')
        plt.xlabel('Predicted Outcome')
        plt.ylabel('Actual Outcome')
        plt.tight_layout()
        plt.savefig('output/confusion_matrix.png')
        plt.close()
        
    except Exception as e:
        logger.error(f"Error plotting confusion matrix: {e}")
        raise

def analyze_results():
    spark = None
    try:
        spark = create_spark_session()
        predictions = spark.read.parquet("output/predictions")
        
        # Calculate basic metrics
        total = predictions.count()
        correct = predictions.filter(col("prediction") == col("match_outcome")).count()
        accuracy = correct / total
        
        # Create confusion matrix with direct aggregation
        matrix = predictions.groupBy("match_outcome").agg(
            count(when(col("prediction") == 0, 1)).alias("pred_0"),
            count(when(col("prediction") == 1, 1)).alias("pred_1"),
            count(when(col("prediction") == 2, 1)).alias("pred_2")
        ).toPandas()
        
        # Convert match_outcome to numeric and sort
        matrix['match_outcome'] = matrix['match_outcome'].astype(int)
        matrix = matrix.sort_values('match_outcome').set_index('match_outcome')
        
        # Log metrics
        logger.info(f"\nModel Performance Analysis:")
        logger.info(f"Total Matches Analyzed: {total:,}")
        logger.info(f"Correct Predictions: {correct:,}")
        logger.info(f"Overall Accuracy: {accuracy:.4f}")
        
        # Calculate per-class metrics
        outcomes = {0: "Home Win", 1: "Draw", 2: "Away Win"}
        for i in range(3):
            col_name = f"pred_{i}"
            class_total = matrix[col_name].sum()
            class_correct = matrix.loc[i, col_name] if i in matrix.index else 0
            class_accuracy = class_correct / class_total if class_total > 0 else 0
            logger.info(f"{outcomes[i]} Accuracy: {class_accuracy:.4f}")
        
        return matrix
        
    finally:
        if spark:
            spark.stop()

def plot_results(matrix):
    plt.figure(figsize=(12, 8))
    
    # Calculate percentages for annotations
    totals = matrix.sum(axis=1)
    percentages = matrix.div(totals, axis=0) * 100
    
    # Create heatmap with percentage annotations
    sns.heatmap(
        matrix,
        annot=percentages.round(1).astype(str) + '%\n(' + matrix.astype(str) + ')',
        fmt='',
        cmap='YlOrRd',
        xticklabels=['Home Win', 'Draw', 'Away Win'],
        yticklabels=['Home Win', 'Draw', 'Away Win']
    )
    
    plt.title('Soccer Match Prediction Confusion Matrix\n(Percentage and Count)', pad=20)
    plt.xlabel('Predicted Outcome')
    plt.ylabel('Actual Outcome')
    
    # Add accuracy summary
    plt.figtext(0.02, 0.02, f'Overall Accuracy: {matrix.values.trace() / matrix.values.sum():.1%}', 
                fontsize=10, ha='left')
    
    plt.tight_layout()
    plt.savefig('output/confusion_matrix.png', dpi=300, bbox_inches='tight')
    plt.close()

def main():
    """Main execution function"""
    spark = None
    
    try:
        # Initialize Spark
        spark = create_spark_session()
        
        # Run analysis
        confusion_df = analyze_predictions(spark)
        
        # Create visualization
        plot_confusion_matrix(confusion_df)
        
        logger.info("\nAnalysis complete! Check output/confusion_matrix.png for visualization")
        
    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        raise
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    try:
        confusion_matrix = analyze_results()
        plot_results(confusion_matrix)
        logger.info("\nAnalysis complete! Check output/confusion_matrix.png")
    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        raise
