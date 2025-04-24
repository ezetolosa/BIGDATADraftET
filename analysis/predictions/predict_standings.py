import logging
import pandas as pd
from pyspark.sql import SparkSession
import numpy as np
import matplotlib
matplotlib.use('Agg')  # Set non-interactive backend before importing pyplot
import matplotlib.pyplot as plt
import seaborn as sns  # Add seaborn import
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LeaguePredictor:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("League Predictor") \
            .config("spark.driver.memory", "4g") \
            .master("local[*]") \
            .getOrCreate()

    def load_data(self):
        """Load historical match data"""
        return self.spark.read.csv(
            "data/processed/matches.csv",
            header=True,
            inferSchema=True
        )

    def predict_season(self, league_name):
        """Predict standings for next season with home/away matches"""
        # Load historical data
        df = self.load_data()
        league_data = df.filter(df.league_name == league_name).toPandas()

        # Get unique teams
        teams = pd.concat([
            league_data['home_team_long_name'],
            league_data['away_team_long_name']
        ]).unique()

        # Initialize team stats
        team_stats = {}
        for team in teams:
            home_games = league_data[league_data['home_team_long_name'] == team]
            away_games = league_data[league_data['away_team_long_name'] == team]

            # Calculate historical performance
            home_goals_avg = home_games['home_team_goal'].mean() or 0
            away_goals_avg = away_games['away_team_goal'].mean() or 0
            home_conceded_avg = home_games['away_team_goal'].mean() or 0
            away_conceded_avg = away_games['home_team_goal'].mean() or 0

            team_stats[team] = {
                'home_goals_avg': home_goals_avg,
                'away_goals_avg': away_goals_avg,
                'home_conceded_avg': home_conceded_avg,
                'away_conceded_avg': away_conceded_avg,
                'predicted_points': 0,
                'games_played': 0,
                'wins': 0,
                'draws': 0,
                'losses': 0,
                'goals_for': 0,
                'goals_against': 0
            }

        # Simulate full season - each team plays against every other team twice
        for home_team in teams:
            for away_team in teams:
                if home_team != away_team:
                    # First leg (home)
                    home_score = np.random.poisson(
                        team_stats[home_team]['home_goals_avg'] * 1.1
                    )
                    away_score = np.random.poisson(
                        team_stats[away_team]['away_goals_avg'] * 0.9
                    )

                    # Update stats for first leg
                    team_stats[home_team]['games_played'] += 1
                    team_stats[away_team]['games_played'] += 1
                    team_stats[home_team]['goals_for'] += home_score
                    team_stats[home_team]['goals_against'] += away_score
                    team_stats[away_team]['goals_for'] += away_score
                    team_stats[away_team]['goals_against'] += home_score

                    if home_score > away_score:
                        team_stats[home_team]['predicted_points'] += 3
                        team_stats[home_team]['wins'] += 1
                        team_stats[away_team]['losses'] += 1
                    elif home_score < away_score:
                        team_stats[away_team]['predicted_points'] += 3
                        team_stats[away_team]['wins'] += 1
                        team_stats[home_team]['losses'] += 1
                    else:
                        team_stats[home_team]['predicted_points'] += 1
                        team_stats[away_team]['predicted_points'] += 1
                        team_stats[home_team]['draws'] += 1
                        team_stats[away_team]['draws'] += 1

                    # Second leg (away)
                    away_score_2 = np.random.poisson(
                        team_stats[home_team]['away_goals_avg'] * 0.9
                    )
                    home_score_2 = np.random.poisson(
                        team_stats[away_team]['home_goals_avg'] * 1.1
                    )

                    # Update stats for second leg
                    team_stats[home_team]['games_played'] += 1
                    team_stats[away_team]['games_played'] += 1
                    team_stats[home_team]['goals_for'] += away_score_2
                    team_stats[home_team]['goals_against'] += home_score_2
                    team_stats[away_team]['goals_for'] += home_score_2
                    team_stats[away_team]['goals_against'] += away_score_2

                    if home_score_2 > away_score_2:
                        team_stats[away_team]['predicted_points'] += 3
                        team_stats[away_team]['wins'] += 1
                        team_stats[home_team]['losses'] += 1
                    elif home_score_2 < away_score_2:
                        team_stats[home_team]['predicted_points'] += 3
                        team_stats[home_team]['wins'] += 1
                        team_stats[away_team]['losses'] += 1
                    else:
                        team_stats[home_team]['predicted_points'] += 1
                        team_stats[away_team]['predicted_points'] += 1
                        team_stats[home_team]['draws'] += 1
                        team_stats[away_team]['draws'] += 1

        # Create standings DataFrame with proper ranking criteria
        standings = pd.DataFrame.from_dict(team_stats, orient='index')
        standings = standings.sort_values(
            by=['predicted_points', 'goals_for', 'goals_against'],  # Multiple criteria
            ascending=[False, False, True]  # Points descending, GF descending, GA ascending
        )
        standings['goal_difference'] = standings['goals_for'] - standings['goals_against']
        standings.index.name = 'Team'

        # Show detailed standings with all tiebreaker criteria
        logger.info(f"\nPredicted {league_name} Standings:")
        for idx, (team, row) in enumerate(standings.iterrows(), 1):
            logger.info(
                f"{idx}. {team:25} | "
                f"P:{row['games_played']:2} | "
                f"W:{row['wins']:2} D:{row['draws']:2} L:{row['losses']:2} | "
                f"GF:{row['goals_for']:3} GA:{row['goals_against']:3} GD:{row['goal_difference']:3} | "
                f"Pts:{int(row['predicted_points'])}"
            )

        # Save predictions
        plots_dir = 'output/plots/league_analysis'
        os.makedirs(plots_dir, exist_ok=True)
        
        # Set style directly with seaborn
        sns.set_style("whitegrid")
        fig, ax = plt.subplots(figsize=(15, 10))
        
        # Create color palette
        colors = sns.color_palette("husl", len(standings))
        bars = ax.bar(standings.index, standings['predicted_points'], color=colors)
        
        # Customize plot
        ax.set_title(f'Predicted {league_name} Standings\nSeason 2016-17', pad=20, size=14)
        ax.set_xlabel('Teams', labelpad=10)
        ax.set_ylabel('Predicted Points', labelpad=10)
        
        # Rotate labels for better readability
        plt.xticks(rotation=45, ha='right')
        
        # Add value labels on top of bars
        for bar in bars:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                    f'{int(height)}',
                    ha='center', va='bottom')
        
        plt.tight_layout()
        
        # Save plot
        output_file = f'{plots_dir}/predicted_standings_{league_name.replace(" ", "_")}.png'
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"\nPredicted {league_name} Standings saved to: {output_file}")
        logger.info("\nTeam Rankings:")
        for idx, (team, row) in enumerate(standings.iterrows(), 1):
            logger.info(f"{idx}. {team}: {int(row['predicted_points'])} points")
        
        return standings

def main():
    predictor = LeaguePredictor()
    
    # List available leagues
    df = predictor.load_data()
    leagues = df.select('league_name').distinct().toPandas()
    
    logger.info("\nAvailable leagues:")
    for idx, league in enumerate(leagues['league_name'], 1):
        logger.info(f"{idx}. {league}")
    
    # Let user select league
    league_idx = int(input("\nSelect league number: ")) - 1
    selected_league = leagues.iloc[league_idx]['league_name']
    
    # Predict standings
    predictor.predict_season(selected_league)

if __name__ == "__main__":
    main()