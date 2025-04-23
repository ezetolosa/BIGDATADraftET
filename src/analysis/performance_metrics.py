from pyspark.sql.functions import col, avg, sum, count
from pyspark.sql.window import Window
import logging

class TeamPerformanceAnalyzer:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.logger = logging.getLogger(__name__)
        
    def calculate_team_metrics(self, df, team_name):
        """Calculate comprehensive team performance metrics"""
        try:
            team_stats = (df
                .filter((col("home_team_long_name") == team_name) | 
                        (col("away_team_long_name") == team_name))
                .withColumn("goals_scored",
                    when(col("home_team_long_name") == team_name, col("home_team_goal"))
                    .otherwise(col("away_team_goal")))
                .withColumn("goals_conceded",
                    when(col("home_team_long_name") == team_name, col("away_team_goal"))
                    .otherwise(col("home_team_goal")))
                .agg(
                    avg("goals_scored").alias("avg_goals_scored"),
                    avg("goals_conceded").alias("avg_goals_conceded"),
                    count("*").alias("total_matches")
                ))
            
            return team_stats
            
        except Exception as e:
            self.logger.error(f"Error calculating team metrics: {e}")
            raise