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
        # Load and convert data properly
        logger.info("Loading match data...")
        df = self.load_data()
        
        logger.info("Processing data for analysis...")
        pandas_df = df.filter(f"league_name = '{league_name}'").toPandas()
        pandas_df['date'] = pd.to_datetime(pandas_df['date'])
        
        current_date = pandas_df['date'].max()
        three_years_ago = current_date - pd.DateOffset(years=3)
        recent_data = pandas_df[pandas_df['date'] >= three_years_ago]
        
        logger.info(f"Analyzing matches from {three_years_ago.strftime('%Y-%m-%d')} to {current_date.strftime('%Y-%m-%d')}")
        
        # Get teams and calculate their averages
        teams = pd.concat([
            recent_data['home_team_long_name'],
            recent_data['away_team_long_name']
        ]).unique()

        # Initialize team stats with all required fields
        team_stats = {}
        for team in teams:
            home_games = recent_data[recent_data['home_team_long_name'] == team]
            away_games = recent_data[recent_data['away_team_long_name'] == team]
            
            # Calculate averages
            home_goals_avg = home_games['home_team_goal'].mean() if len(home_games) > 0 else 0
            away_goals_avg = away_games['away_team_goal'].mean() if len(away_games) > 0 else 0
            
            team_stats[team] = {
                'predicted_points': 0,
                'games_played': 0,
                'wins': 0,
                'draws': 0,
                'losses': 0,
                'goals_for': 0,
                'goals_against': 0,
                'home_goals_avg': home_goals_avg,
                'away_goals_avg': away_goals_avg,
                'form': 0.0
            }

        # Now the simulation code can safely access 'home_goals_avg'
        for home_team in teams:
            for away_team in teams:
                if home_team != away_team:
                    # Simulate match using the averages
                    home_score = np.random.poisson(team_stats[home_team]['home_goals_avg'] * 1.1)
                    away_score = np.random.poisson(team_stats[away_team]['away_goals_avg'] * 0.9)
                    
                    # Update stats
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

        # Plotting section
        plots_dir = 'output/plots/league_analysis'
        os.makedirs(plots_dir, exist_ok=True)

        # Set professional style with white background
        plt.style.use('seaborn-v0_8-darkgrid')
        fig, ax = plt.subplots(figsize=(15, 10), facecolor='white')
        ax.set_facecolor('#f8f9fa')

        # Create professional color palette
        # Using darker blue gradient for better visibility
        n_teams = len(standings)
        colors = plt.cm.YlOrRd(np.linspace(0.8, 0.3, n_teams))  # Darker gradient

        # Create bars with professional colors
        bars = ax.bar(standings.index, standings['predicted_points'], color=colors)

        # Customize plot with professional styling
        ax.set_title(f'Predicted {league_name} Standings\nSeason 2016-17', 
                     pad=20, 
                     size=16, 
                     fontweight='bold',
                     color='#2f2f2f')

        # Customize axes
        ax.set_xlabel('Teams', labelpad=10, fontsize=12, color='#2f2f2f')
        ax.set_ylabel('Predicted Points', labelpad=10, fontsize=12, color='#2f2f2f')

        # Rotate labels for better readability
        plt.xticks(rotation=45, ha='right', fontsize=10, color='#2f2f2f')
        plt.yticks(fontsize=10, color='#2f2f2f')

        # Add value labels on bars with contrast check
        for bar in bars:
            height = bar.get_height()
            color = 'white' if height > standings['predicted_points'].mean() else '#2f2f2f'
            ax.text(bar.get_x() + bar.get_width()/2., height,
                    f'{int(height)}',
                    ha='center', va='bottom',
                    fontsize=10,
                    fontweight='bold',
                    color=color)

        # Add grid for better readability
        ax.grid(True, axis='y', linestyle='--', alpha=0.2, color='#2f2f2f')

        # Remove top and right spines
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['left'].set_color('#2f2f2f')
        ax.spines['bottom'].set_color('#2f2f2f')

        # Adjust layout
        plt.tight_layout()

        # Save plot with high quality
        output_file = f'{plots_dir}/predicted_standings_{league_name.replace(" ", "_")}.png'
        plt.savefig(output_file, 
                    dpi=300, 
                    bbox_inches='tight',
                    facecolor='white',
                    edgecolor='none')
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