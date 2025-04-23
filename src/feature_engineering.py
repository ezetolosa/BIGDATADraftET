# src/feature_engineering.py
from pyspark.sql.functions import col, avg, sum, when
from pyspark.sql.window import Window
import logging

class FeatureEngineer:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.logger = logging.getLogger(__name__)

    def create_team_features(self, df):
        try:
            # Sort by date for time-based calculations
            df = df.orderBy("date")

            # Create window specs
            home_window = Window.partitionBy("home_team_long_name").orderBy("date").rowsBetween(-5, -1)
            away_window = Window.partitionBy("away_team_long_name").orderBy("date").rowsBetween(-5, -1)

            # Calculate rolling averages
            df = df.withColumn("home_team_goal_rolling_avg", avg("home_team_goal").over(home_window))
            df = df.withColumn("away_team_goal_rolling_avg", avg("away_team_goal").over(away_window))

            # Calculate win streaks
            df = df.withColumn(
                "home_team_win_streak",
                sum(when(col("home_team_goal") > col("away_team_goal"), 1).otherwise(0)).over(home_window)
            )
            df = df.withColumn(
                "away_team_win_streak",
                sum(when(col("away_team_goal") > col("home_team_goal"), 1).otherwise(0)).over(away_window)
            )

            # Calculate total goals scored
            df = df.withColumn(
                "home_team_goals_scored",
                sum("home_team_goal").over(home_window)
            )
            df = df.withColumn(
                "away_team_goals_scored",
                sum("away_team_goal").over(away_window)
            )

            # Calculate form (points per game)
            df = df.withColumn(
                "home_team_form",
                avg(
                    when(col("home_team_goal") > col("away_team_goal"), 3.0)
                    .when(col("home_team_goal") == col("away_team_goal"), 1.0)
                    .otherwise(0.0)
                ).over(home_window)
            )
            df = df.withColumn(
                "away_team_form",
                avg(
                    when(col("away_team_goal") > col("home_team_goal"), 3.0)
                    .when(col("home_team_goal") == col("away_team_goal"), 1.0)
                    .otherwise(0.0)
                ).over(away_window)
            )

            return df

        except Exception as e:
            self.logger.error(f"Feature engineering error: {e}")
            raise
