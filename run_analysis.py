# run_analysis.py – Interactive analysis tool
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, when
from src.model_training import ModelTrainer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spark = SparkSession.builder.appName("SoccerUserAnalysis").getOrCreate()
df = spark.read.parquet("data/processed/featured_matches.parquet")

# Step 1: Choose league
leagues = [row["league_name"] for row in df.select("league_name").distinct().collect()]
print("Available leagues:", ", ".join(leagues))
focus_league = input("Choose a league (or press Enter for ALL): ").strip()
if focus_league:
    df = df.filter(col("league_name") == focus_league)

# Step 2: Choose club
clubs = df.select("home_team_long_name").distinct().rdd.flatMap(lambda x: x).collect()
print("Clubs:", ", ".join(clubs))
focus_club = input("Choose a club (or press Enter for ALL): ").strip()
if focus_club and focus_club.lower() != "all":
    df = df.filter((col("home_team_long_name") == focus_club) | (col("away_team_long_name") == focus_club))

# Step 3: Choose analysis
choice = input("What do you want to do? (1 = KPIs, 2 = Predict Result, 3 = Predict Goals): ").strip()

if choice == "1":
    print("\n--- KPI Report ---")
    df.select("home_team_goal_rolling_avg", "away_team_goal_rolling_avg",
              "home_team_conceded_avg", "away_team_conceded_avg",
              "home_team_form", "away_team_form").show(10)

elif choice == "2":
    print("\n--- Match Outcome Probabilities ---")
    model_trainer = ModelTrainer(spark)
    model = model_trainer.load_model("models/result_classifier")
    pred = model.transform(df)

    pred.select("home_team_long_name", "away_team_long_name", "probability").show(10, truncate=False)

elif choice == "3":
    print("\n--- Goal Predictions ---")
    model_trainer = ModelTrainer(spark)
    model = model_trainer.load_model("models/goal_regressor")
    pred = model.transform(df)

    pred.select("home_team_long_name", "away_team_long_name", "prediction").show(10)

else:
    print("Invalid option.")

spark.stop()
