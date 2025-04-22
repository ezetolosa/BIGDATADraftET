# src/data_processing.py
from pyspark.sql.functions import col, to_date
import logging

class DataProcessor:
    def __init__(self, spark_session):
        self.spark = spark_session

    def load_and_clean_match_data(self, focus_league=None, focus_club=None):
        try:
            df = self.spark.read.csv("data/raw/match.csv", header=True, inferSchema=True)

            # Filter essential columns
            df = df.dropna(subset=["home_team_goal", "away_team_goal"])

            # Convert types
            numeric_columns = ["home_team_goal", "away_team_goal"]
            for col_name in numeric_columns:
                df = df.withColumn(col_name, col(col_name).cast("integer"))

            df = df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
            df = df.dropDuplicates()

            # Apply filters if specified
            if focus_league:
                df = df.filter(col("league_name") == focus_league)

            if focus_club:
                df = df.filter((col("home_team_long_name") == focus_club) |
                               (col("away_team_long_name") == focus_club))

            return df

        except Exception as e:
            logging.error(f"Data processing error: {e}")
            raise