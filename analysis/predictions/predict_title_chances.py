import logging
import pandas as pd
from pyspark.sql import SparkSession
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
import os
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TitlePredictor:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("Title Predictor") \
            .config("spark.driver.memory", "4g") \
            .master("local[*]") \
            .getOrCreate()

    def calculate_team_metrics(self, df, team, last_n_years=3):
        """Calculate team performance metrics based on historical data"""
        current_date = df['date'].max()
        cutoff_date = current_date - pd.DateOffset(years=last_n_years)
        recent_df = df[df['date'] > cutoff_date]
        
        metrics = {}
        
        # Recent form (last N years)
        home_games = recent_df[recent_df['home_team_long_name'] == team]
        away_games = recent_df[recent_df['away_team_long_name'] == team]
        
        total_games = len(home_games) + len(away_games)
        if total_games == 0:
            return None
            
        # Calculate win rates
        home_wins = len(home_games[home_games['home_team_goal'] > home_games['away_team_goal']])
        away_wins = len(away_games[away_games['away_team_goal'] > away_games['home_team_goal']])
        
        metrics['win_rate'] = (home_wins + away_wins) / total_games
        
        # Calculate goal metrics
        metrics['avg_goals_scored'] = (
            home_games['home_team_goal'].mean() + 
            away_games['away_team_goal'].mean()
        ) / 2
        
        metrics['avg_goals_conceded'] = (
            home_games['away_team_goal'].mean() + 
            away_games['home_team_goal'].mean()
        ) / 2
        
        return metrics

    def get_head_to_head_stats(self, df, team1, team2, last_n_years=3):
        """Calculate head-to-head statistics between two teams"""
        current_date = df['date'].max()
        cutoff_date = current_date - pd.DateOffset(years=last_n_years)
        recent_df = df[df['date'] > cutoff_date]
        
        # Get matches between these teams
        h2h_matches = recent_df[
            ((recent_df['home_team_long_name'] == team1) & 
             (recent_df['away_team_long_name'] == team2)) |
            ((recent_df['home_team_long_name'] == team2) & 
             (recent_df['away_team_long_name'] == team1))
        ]
        
        if len(h2h_matches) == 0:
            return 0.5  # Default to even chances if no history
            
        # Calculate team1's win rate against team2
        team1_wins = len(
            h2h_matches[
                ((h2h_matches['home_team_long_name'] == team1) & 
                 (h2h_matches['home_team_goal'] > h2h_matches['away_team_goal'])) |
                ((h2h_matches['away_team_long_name'] == team1) & 
                 (h2h_matches['away_team_goal'] > h2h_matches['home_team_goal']))
            ]
        )
        
        return team1_wins / len(h2h_matches)

    def predict_title_chances(self, league_name):
        """Predict title chances based on historical performance"""
        logger.info("Loading and processing data...")
        
        # Load data more efficiently
        df = self.spark.read.csv(
            "data/processed/matches.csv",
            header=True,
            inferSchema=True
        ).filter(f"league_name = '{league_name}'").toPandas()
        
        df['date'] = pd.to_datetime(df['date'])
        current_date = df['date'].max()
        cutoff_date = current_date - pd.DateOffset(years=3)
        
        # Filter recent data once
        recent_data = df[df['date'] > cutoff_date]
        
        logger.info("Calculating team metrics...")
        teams = pd.concat([
            recent_data['home_team_long_name'],
            recent_data['away_team_long_name']
        ]).unique()
        
        # Pre-calculate all team metrics
        team_metrics = {}
        for team in teams:
            home_games = recent_data[recent_data['home_team_long_name'] == team]
            away_games = recent_data[recent_data['away_team_long_name'] == team]
            
            if len(home_games) + len(away_games) > 0:
                home_wins = len(home_games[home_games['home_team_goal'] > home_games['away_team_goal']])
                away_wins = len(away_games[away_games['away_team_goal'] > away_games['home_team_goal']])
                total_games = len(home_games) + len(away_games)
                
                team_metrics[team] = {
                    'win_rate': (home_wins + away_wins) / total_games,
                    'home_win_rate': home_wins / len(home_games) if len(home_games) > 0 else 0,
                    'away_win_rate': away_wins / len(away_games) if len(away_games) > 0 else 0
                }
        
        logger.info(f"Running {500} simulations...")  # Reduced from 1000 for speed
        titles = {team: 0 for team in team_metrics.keys()}
        
        # Vectorized simulation
        for _ in range(500):  # Reduced iterations
            season_points = {team: 0 for team in team_metrics.keys()}
            
            for home in team_metrics.keys():
                for away in team_metrics.keys():
                    if home != away:
                        # Simplified probability calculation
                        win_prob = (
                            team_metrics[home]['home_win_rate'] * 1.1 +
                            (1 - team_metrics[away]['away_win_rate']) * 0.9
                        ) / 2
                        
                        result = np.random.random()
                        if result < win_prob:
                            season_points[home] += 3
                        elif result > 0.85:
                            season_points[home] += 1
                            season_points[away] += 1
                        else:
                            season_points[away] += 3
            
            titles[max(season_points.items(), key=lambda x: x[1])[0]] += 1
        
        # Calculate percentages
        title_chances = {
            team: (wins/500)*100 
            for team, wins in titles.items()
        }
        
        logger.info("Creating visualization...")
        # Sort teams by title chances
        sorted_chances = dict(sorted(title_chances.items(), key=lambda x: x[1], reverse=True))

        # Create visualization with sorted teams
        plt.figure(figsize=(12, 8))
        teams = list(sorted_chances.keys())
        chances = list(sorted_chances.values())

        # Create bar plot with sorted values
        colors = sns.color_palette("husl", len(teams))
        bars = plt.barh(teams, chances, color=colors)
        
        plt.title(f'Title Chances for {league_name} (Based on 2014-2016 data)', pad=20)
        plt.xlabel('Chance to Win Title (%)')
        plt.ylabel('Teams')
        
        # Add percentage labels on bars
        for bar in bars:
            width = bar.get_width()
            plt.text(width + 0.5, bar.get_y() + bar.get_height()/2,
                    f'{width:.1f}%',
                    ha='left', va='center')

        plt.tight_layout()
        
        # Save plot
        output_file = f'output/plots/league_analysis/title_chances_{league_name.replace(" ", "_")}.png'
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        plt.close()

        # Display sorted results
        logger.info(f"\nPredicted Title Chances for {league_name}:")
        logger.info("Based on historical performance 2014-2016")
        logger.info("-" * 50)
        for team, chance in sorted_chances.items():
            logger.info(f"{team:30} {chance:.1f}%")

        return sorted_chances

def show_menu():
    """Display main menu options"""
    print("\n=== Title Prediction Menu ===")
    print("1. Select League")
    print("2. Exit")
    return input("Choice (1-2): ")

def select_league(leagues):
    """Display league selection menu"""
    print("\nAvailable leagues:")
    for idx, league in enumerate(leagues['league_name'], 1):
        logger.info(f"{idx}. {league}")
    
    while True:
        try:
            choice = input("\nSelect league number (or 'b' for main menu): ")
            if choice.lower() == 'b':
                return None
            league_idx = int(choice) - 1
            if 0 <= league_idx < len(leagues):
                return leagues.iloc[league_idx]['league_name']
            else:
                print("Invalid league number. Please try again.")
        except ValueError:
            print("Invalid input. Please enter a number or 'b'.")

def main():
    predictor = TitlePredictor()
    df = predictor.spark.read.csv("data/processed/matches.csv", header=True)
    leagues = df.select('league_name').distinct().toPandas()

    while True:
        choice = show_menu()
        
        if choice == '1':
            selected_league = select_league(leagues)
            if selected_league:
                predictor.predict_title_chances(selected_league)
                input("\nPress Enter to continue...")
        elif choice == '2':
            logger.info("Exiting program...")
            break
        else:
            print("Invalid choice. Please try again.")

    predictor.spark.stop()

if __name__ == "__main__":
    main()