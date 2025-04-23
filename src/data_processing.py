# src/data_processing.py
from pyspark.sql.functions import col, to_date, when
from pyspark.sql import SparkSession
import logging
import os

class DataProcessor:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.logger = logging.getLogger(__name__)  # Add proper logger

    def validate_data(self, df):
        """Validate input data quality"""
        required_columns = [
            "date", 
            "home_team_api_id",  # Note: These are the original column names from SQLite
            "away_team_api_id",
            "home_team_goal",
            "away_team_goal",
            "league_id"
        ]
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")

    def load_and_clean_match_data(self, focus_league=None, focus_club=None):
        try:
            # Load all necessary CSV files
            matches_df = self.spark.read.csv("data/raw/match.csv", header=True, inferSchema=True)
            teams_df = self.spark.read.csv("data/raw/team.csv", header=True, inferSchema=True)
            leagues_df = self.spark.read.csv("data/raw/league.csv", header=True, inferSchema=True)

            # Validate raw match data
            self.validate_data(matches_df)

            # Join dataframes to get complete match information
            df = matches_df.join(
                teams_df.select("team_api_id", "team_long_name").alias("home_team"),
                matches_df.home_team_api_id == col("home_team.team_api_id")
            ).join(
                teams_df.select("team_api_id", "team_long_name").alias("away_team"),
                matches_df.away_team_api_id == col("away_team.team_api_id")
            ).join(
                leagues_df.select("id", "name").alias("league"),
                matches_df.league_id == col("league.id")
            )

            # Filter essential columns
            df = df.dropna(subset=["home_team_goal", "away_team_goal"])

            # Convert types
            numeric_columns = ["home_team_goal", "away_team_goal"]
            for col_name in numeric_columns:
                df = df.withColumn(col_name, col(col_name).cast("integer"))

            df = df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
            df = df.dropDuplicates()

            # Filter by league if specified
            if focus_league:
                df = df.filter(col("league.name") == focus_league)

            if focus_club:
                df = df.filter((col("home_team.team_long_name") == focus_club) |
                               (col("away_team.team_long_name") == focus_club))

            # Create numeric match outcome (0 for HOME_WIN, 1 for DRAW, 2 for AWAY_WIN)
            df = df.withColumn("match_outcome",
                when(col("home_team_goal") > col("away_team_goal"), 0.0)
                .when(col("home_team_goal") < col("away_team_goal"), 2.0)
                .otherwise(1.0)
            )

            # Select and rename columns
            final_df = df.select(
                col("date"),
                col("league.name").alias("league_name"),
                col("home_team.team_long_name").alias("home_team_long_name"),
                col("away_team.team_long_name").alias("away_team_long_name"),
                col("home_team_goal"),
                col("away_team_goal"),
                col("match_outcome").cast("double")  # Ensure numeric type
            )

            # Cache the dataframe for better performance
            final_df.cache()

            self.logger.info(f"Processed {final_df.count()} matches successfully")
            return final_df

        except Exception as e:
            self.logger.error(f"Data processing error: {e}", exc_info=True)
            raise