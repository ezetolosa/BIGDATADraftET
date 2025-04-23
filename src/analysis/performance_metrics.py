import pandas as pd
import logging
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

class TeamPerformanceAnalyzer:
    def __init__(self, spark_session=None):
        self.spark = spark_session or SparkSession.builder \
            .appName("Team Performance Analysis") \
            .config("spark.driver.memory", "1g") \
            .master("local[*]") \
            .getOrCreate()
    
    def analyze_team_performance(self, team_name):
        """Analyze performance metrics for a specific team"""
        try:
            # Read Parquet data
            matches_df = self.spark.read.parquet("output/predictions")
            
            # Filter team matches
            team_matches = matches_df.filter(
                (matches_df.home_team_long_name == team_name) | 
                (matches_df.away_team_long_name == team_name)
            )
            
            return team_matches.toPandas()
            
        except Exception as e:
            logger.error(f"Error analyzing team performance: {str(e)}")
            return None