# src/data_processing.py
from pyspark.sql.functions import col, to_date, when
import logging

class DataProcessor:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.logger = logging.getLogger(__name__)  # Add proper logger

    def validate_data(self, df):
        """Validate input data quality"""
        required_columns = ["date", "home_team_goal", "away_team_goal", 
                          "home_team_long_name", "away_team_long_name"]
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")

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

            # Add data validation
            self.validate_data(df)

            # Add match outcome column
            df = df.withColumn("match_outcome", 
                when(col("home_team_goal") > col("away_team_goal"), "HOME_WIN")
                .when(col("home_team_goal") < col("away_team_goal"), "AWAY_WIN")
                .otherwise("DRAW"))

            # Add total goals column
            df = df.withColumn("total_goals", 
                col("home_team_goal") + col("away_team_goal"))

            # Cache the dataframe for better performance
            df.cache()

            self.logger.info(f"Processed {df.count()} matches successfully")
            return df

        except Exception as e:
            self.logger.error(f"Data processing error: {e}", exc_info=True)
            raise