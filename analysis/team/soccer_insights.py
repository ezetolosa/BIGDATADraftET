import logging
import pandas as pd
from pyspark.sql import SparkSession
import matplotlib
matplotlib.use('Agg')  # Set backend to Agg before importing pyplot
import matplotlib.pyplot as plt
import seaborn as sns
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    return (SparkSession.builder
            .appName("Soccer Insights")
            .config("spark.driver.memory", "1g")
            .master("local[*]")
            .getOrCreate())

def load_match_data():
    spark = create_spark_session()
    return spark.read.parquet("output/predictions").toPandas()

def analyze_team_performance(matches_df, team_name):
    """Analyze detailed performance metrics for a team"""
    team_matches = matches_df[
        (matches_df['home_team_long_name'] == team_name) | 
        (matches_df['away_team_long_name'] == team_name)
    ]
    
    # Home performance
    home_matches = team_matches[team_matches['home_team_long_name'] == team_name]
    home_wins = len(home_matches[home_matches['match_outcome'] == 0])
    home_draws = len(home_matches[home_matches['match_outcome'] == 1])
    home_losses = len(home_matches[home_matches['match_outcome'] == 2])
    
    # Away performance
    away_matches = team_matches[team_matches['away_team_long_name'] == team_name]
    away_wins = len(away_matches[away_matches['match_outcome'] == 2])
    away_draws = len(away_matches[away_matches['match_outcome'] == 1])
    away_losses = len(away_matches[away_matches['match_outcome'] == 0])
    
    # Calculate statistics
    total_matches = len(team_matches)
    win_rate = (home_wins + away_wins) / total_matches
    home_win_rate = home_wins / len(home_matches) if len(home_matches) > 0 else 0
    away_win_rate = away_wins / len(away_matches) if len(away_matches) > 0 else 0
    
    # Display insights
    logger.info(f"\nTeam Analysis: {team_name}")
    logger.info(f"Total Matches: {total_matches}")
    logger.info(f"\nHome Performance:")
    logger.info(f"Wins: {home_wins}, Draws: {home_draws}, Losses: {home_losses}")
    logger.info(f"Home Win Rate: {home_win_rate:.2%}")
    logger.info(f"\nAway Performance:")
    logger.info(f"Wins: {away_wins}, Draws: {away_draws}, Losses: {away_losses}")
    logger.info(f"Away Win Rate: {away_win_rate:.2%}")
    logger.info(f"\nOverall Win Rate: {win_rate:.2%}")
    
    return {
        'home_results': [home_wins, home_draws, home_losses],
        'away_results': [away_wins, away_draws, away_losses],
        'win_rates': [home_win_rate, away_win_rate, win_rate]
    }

def plot_team_performance(stats, team_name):
    """Create visualization of team performance and save to file"""
    plt.figure(figsize=(12, 6))
    
    # Home performance
    plt.subplot(1, 2, 1)
    plt.pie(stats['home_results'], labels=['Wins', 'Draws', 'Losses'], 
            autopct='%1.1f%%', colors=['green', 'yellow', 'red'])
    plt.title(f'{team_name} Home Performance')
    
    # Away performance
    plt.subplot(1, 2, 2)
    plt.pie(stats['away_results'], labels=['Wins', 'Draws', 'Losses'],
            autopct='%1.1f%%', colors=['green', 'yellow', 'red'])
    plt.title(f'{team_name} Away Performance')
    
    plt.tight_layout()
    
    # Create output directory if it doesn't exist
    os.makedirs('output/plots', exist_ok=True)
    
    # Save plot to file
    filename = f'output/plots/{team_name.replace(" ", "_")}_performance.png'
    plt.savefig(filename)
    plt.close()
    
    logger.info(f"\nPlot saved as: {filename}")

def compare_teams(matches_df, team1, team2):
    """Compare performance metrics between two teams"""
    team1_stats = analyze_team_performance(matches_df, team1)
    team2_stats = analyze_team_performance(matches_df, team2)
    
    # Plot comparison
    win_rates = pd.DataFrame({
        'Home': [team1_stats['win_rates'][0], team2_stats['win_rates'][0]],
        'Away': [team1_stats['win_rates'][1], team2_stats['win_rates'][1]],
        'Overall': [team1_stats['win_rates'][2], team2_stats['win_rates'][2]]
    }, index=[team1, team2])
    
    plt.figure(figsize=(10, 6))
    win_rates.plot(kind='bar')
    plt.title('Win Rate Comparison')
    plt.ylabel('Win Rate')
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    # Save comparison plot
    filename = f'output/plots/comparison_{team1.replace(" ", "_")}_{team2.replace(" ", "_")}.png'
    plt.savefig(filename)
    plt.close()
    
    logger.info(f"\nComparison plot saved as: {filename}")

def display_teams_by_league(matches_df):
    """Display teams organized by league and return mappings"""
    # Get unique leagues and their teams
    leagues = matches_df.groupby('league_name')['home_team_long_name'].unique()
    
    # Sort leagues and teams
    team_to_number = {}
    current_number = 1
    
    logger.info("\nAvailable Leagues and Teams:")
    for league in sorted(leagues.index):
        logger.info(f"\n{league}:")
        for team in sorted(leagues[league]):
            logger.info(f"{current_number}. {team}")
            team_to_number[current_number] = team
            current_number += 1
    
    return team_to_number

def get_team_choice(team_to_number, prompt):
    """Get team selection with validation"""
    while True:
        try:
            choice = int(input(prompt))
            if 0 < choice <= len(team_to_number):
                return team_to_number[choice]
            else:
                logger.info("Invalid team number. Please try again.")
        except ValueError:
            logger.info("Please enter a valid number.")

def plot_team_analysis(team_data, team_name):
    """Generate and save analysis plots for a team"""
    plots_dir = 'output/plots/team_insights'
    os.makedirs(plots_dir, exist_ok=True)
    
    # Create figure for team analysis
    plt.figure(figsize=(10, 6))
    # ...existing code...
    plt.savefig(f"{plots_dir}/{team_name.replace(' ', '_')}_analysis.png")
    plt.close()
    
    logger.info(f"✅ Plots saved to {plots_dir}")

def create_team_performance_plot(team_data, team_name):
    """Generate and save team performance visualization"""
    # Create plots directory if it doesn't exist
    plot_dir = os.path.join('output', 'plots', 'team_insights')
    os.makedirs(plot_dir, exist_ok=True)

    # Create plot
    plt.figure(figsize=(12, 8))
    # ...existing plotting code...

    # Save plot to team_insights directory
    plot_path = os.path.join(plot_dir, f"{team_name.replace(' ', '_')}_performance.png")
    plt.savefig(plot_path)
    plt.close()

    logger.info(f"✅ Plot saved to: {plot_path}")
    return plot_path

def main():
    matches_df = load_match_data()
    
    while True:
        print("\nSoccer Insights Menu:")
        print("1. Analyze Single Team")
        print("2. Compare Two Teams")
        print("3. Exit")
        
        choice = input("\nSelect option: ")
        
        if choice == '1':
            team_to_number = display_teams_by_league(matches_df)
            team = get_team_choice(team_to_number, "\nSelect team number: ")
            stats = analyze_team_performance(matches_df, team)
            plot_team_performance(stats, team)
            
        elif choice == '2':
            team_to_number = display_teams_by_league(matches_df)
            team1 = get_team_choice(team_to_number, "\nSelect first team number: ")
            logger.info("\nSelect second team:")
            team2 = get_team_choice(team_to_number, "Select second team number: ")
            compare_teams(matches_df, team1, team2)
            
        elif choice == '3':
            break

if __name__ == "__main__":
    main()