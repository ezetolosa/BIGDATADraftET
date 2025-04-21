# src/feature_engineering.py
from pyspark.sql.functions import avg, when, col
from pyspark.sql.window import Window
import logging

class FeatureEngineer:
    def __init__(self, spark_session):
        self.spark = spark_session

    def create_team_features(self, df):
        try:
            # Sort by date for rolling calculations
            df = df.orderBy("date")

            # Add result indicators (for form)
            df = df.withColumn("home_result",
                when(col("home_team_goal") > col("away_team_goal"), 1.0)
                .when(col("home_team_goal") < col("away_team_goal"), 0.0)
                .otherwise(0.5))

            df = df.withColumn("away_result",
                when(col("away_team_goal") > col("home_team_goal"), 1.0)
                .when(col("away_team_goal") < col("home_team_goal"), 0.0)
                .otherwise(0.5))

            # Rolling windows
            home_window = Window.partitionBy("home_team_api_id").orderBy("date").rowsBetween(-5, -1)
            away_window = Window.partitionBy("away_team_api_id").orderBy("date").rowsBetween(-5, -1)

            df = df.withColumn("home_team_goal_rolling_avg", avg("home_team_goal").over(home_window))
            df = df.withColumn("away_team_goal_rolling_avg", avg("away_team_goal").over(away_window))

            df = df.withColumn("home_team_conceded_avg", avg("away_team_goal").over(home_window))
            df = df.withColumn("away_team_conceded_avg", avg("home_team_goal").over(away_window))

            df = df.withColumn("home_team_form", avg("home_result").over(home_window))
            df = df.withColumn("away_team_form", avg("away_result").over(away_window))

            return df

        except Exception as e:
            logging.error(f"Feature engineering error: {e}")
            raise
