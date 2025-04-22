# notebooks/eda_visualization.py
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Load the processed predictions (adjust path if needed)
pred_df = pd.read_csv("output/predictions.csv")
goals_df = pd.read_csv("output/goal_predictions.csv") if 'goal_predictions.csv' in os.listdir('output') else None

# Plot 1: Probability Distribution
plt.figure(figsize=(8, 5))
sns.histplot(pred_df["prob_home_win"], bins=10, color="blue", label="Home Win", kde=True)
sns.histplot(pred_df["prob_draw"], bins=10, color="gray", label="Draw", kde=True)
sns.histplot(pred_df["prob_away_win"], bins=10, color="red", label="Away Win", kde=True)
plt.title("Prediction Probability Distributions")
plt.xlabel("Probability")
plt.ylabel("Match Count")
plt.legend()
plt.tight_layout()
plt.savefig("output/probability_distribution.png")
plt.show()

# Plot 2: Top 10 Most Confident Predictions
pred_df["max_confidence"] = pred_df[["prob_home_win", "prob_draw", "prob_away_win"]].max(axis=1)
top10 = pred_df.sort_values("max_confidence", ascending=False).head(10)
plt.figure(figsize=(10, 6))
sns.barplot(data=top10, x="max_confidence", y="home_team_api_id", hue="prediction")
plt.title("Top 10 Most Confident Predictions")
plt.xlabel("Confidence")
plt.ylabel("Match (Home Team ID)")
plt.tight_layout()
plt.savefig("output/top_confident_predictions.png")
plt.show()

# Plot 3: Goal Predictions Distribution (if available)
if goals_df is not None:
    plt.figure(figsize=(8, 5))
    sns.histplot(goals_df["prediction"], bins=10, kde=True, color="green")
    plt.title("Distribution of Predicted Goals")
    plt.xlabel("Predicted Goals")
    plt.ylabel("Frequency")
    plt.tight_layout()
    plt.savefig("output/goal_predictions_distribution.png")
    plt.show()

print("✅ Visualizations saved to output/ folder.")
