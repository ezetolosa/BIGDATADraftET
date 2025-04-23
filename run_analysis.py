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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create Spark session with temp directory cleanup"""
    # Set custom temp directory
    temp_dir = tempfile.mkdtemp(prefix="spark_")
    
    spark = SparkSession.builder \
        .appName("Soccer Analysis") \
        .config("spark.driver.host", "localhost") \
        .config("spark.driver.bindAddress", "localhost") \
        .config("spark.local.dir", temp_dir) \
        .getOrCreate()
        
    return spark, temp_dir

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

def main():
    # Setup logging
    setup_logging()
    
    # Load configuration
    with open('config/settings.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    # Initialize Spark session and components
    spark = SparkSession.builder.appName("SoccerAnalytics").getOrCreate()
    processor = DataProcessor(spark)
    engineer = FeatureEngineer(spark)
    analyzer = TeamPerformanceAnalyzer(spark)
    
    # Get user input
    league = input("Enter league name: ")
    team = input("Enter team name: ")
    
    # Process data pipeline
    df = processor.load_and_clean_match_data(focus_league=league)
    df_with_features = engineer.create_team_features(df)
    team_stats = analyzer.calculate_team_metrics(df_with_features, team)
    
    # Visualize results
    plot_team_performance(team_stats.toPandas(), team)
    
if __name__ == "__main__":
    main()
