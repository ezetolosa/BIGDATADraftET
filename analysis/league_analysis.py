import logging
import pandas as pd
from pyspark.sql import SparkSession
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    return (SparkSession.builder
            .appName("League Analysis")
            .config("spark.driver.memory", "1g")
            .master("local[*]")
            .getOrCreate())

def load_match_data():
    spark = create_spark_session()
    return spark.read.parquet("output/predictions").toPandas()

def create_league_plots(matches_df, league_name):
    """Generate comprehensive league statistics plots"""
    league_matches = matches_df[matches_df['league_name'] == league_name]
    
    # Create plots directory
    os.makedirs('output/plots/league_analysis', exist_ok=True)
    base_path = f'output/plots/league_analysis/{league_name.replace(" ", "_")}'
    
    # 1. Goals Distribution Plot
    plt.figure(figsize=(12, 6))
    total_goals = league_matches['home_team_goal'] + league_matches['away_team_goal']
    plt.hist(total_goals, bins=range(0, int(max(total_goals)) + 2, 1), 
            alpha=0.7, color='blue', edgecolor='black')
    plt.title(f'Goals Distribution in {league_name}')
    plt.xlabel('Total Goals per Match')
    plt.ylabel('Frequency')
    plt.savefig(f'{base_path}_goals_dist.png')
    plt.close()

    # 2. Home vs Away Goals
    plt.figure(figsize=(10, 6))
    data = [league_matches['home_team_goal'], league_matches['away_team_goal']]
    plt.boxplot(data, labels=['Home Teams', 'Away Teams'])
    plt.title(f'Home vs Away Goals in {league_name}')
    plt.ylabel('Goals Scored')
    plt.savefig(f'{base_path}_home_away_goals.png')
    plt.close()

    # 3. Team Performance Heatmap
    teams = sorted(league_matches['home_team_long_name'].unique())
    performance_matrix = pd.DataFrame(0, index=teams, columns=['Wins', 'Draws', 'Losses', 'Goals For', 'Goals Against'])
    
    for team in teams:
        # Home games
        home_games = league_matches[league_matches['home_team_long_name'] == team]
        # Away games
        away_games = league_matches[league_matches['away_team_long_name'] == team]
        
        performance_matrix.loc[team, 'Wins'] = (
            len(home_games[home_games['match_outcome'] == 0]) +
            len(away_games[away_games['match_outcome'] == 2])
        )
        performance_matrix.loc[team, 'Draws'] = (
            len(home_games[home_games['match_outcome'] == 1]) +
            len(away_games[away_games['match_outcome'] == 1])
        )
        performance_matrix.loc[team, 'Losses'] = (
            len(home_games[home_games['match_outcome'] == 2]) +
            len(away_games[away_games['match_outcome'] == 0])
        )
        performance_matrix.loc[team, 'Goals For'] = (
            home_games['home_team_goal'].sum() +
            away_games['away_team_goal'].sum()
        )
        performance_matrix.loc[team, 'Goals Against'] = (
            home_games['away_team_goal'].sum() +
            away_games['home_team_goal'].sum()
        )

    plt.figure(figsize=(12, 8))
    sns.heatmap(performance_matrix, annot=True, fmt='.0f', cmap='YlOrRd')
    plt.title(f'Team Performance Matrix - {league_name}')
    plt.xticks(rotation=45)
    plt.yticks(rotation=0)
    plt.tight_layout()
    plt.savefig(f'{base_path}_performance_matrix.png')
    plt.close()

    # 4. Win Percentage by Team
    plt.figure(figsize=(12, 6))
    total_games = performance_matrix['Wins'] + performance_matrix['Draws'] + performance_matrix['Losses']
    win_pct = (performance_matrix['Wins'] / total_games * 100).sort_values(ascending=False)
    
    plt.bar(range(len(win_pct)), win_pct)
    plt.xticks(range(len(win_pct)), win_pct.index, rotation=45, ha='right')
    plt.title(f'Win Percentage by Team - {league_name}')
    plt.ylabel('Win Percentage (%)')
    plt.tight_layout()
    plt.savefig(f'{base_path}_win_percentages.png')
    plt.close()

    return base_path

def analyze_league(matches_df, league_name):
    """Analyze league statistics with enhanced visualizations"""
    league_matches = matches_df[matches_df['league_name'] == league_name]
    
    # Calculate league statistics
    total_matches = len(league_matches)
    home_wins = len(league_matches[league_matches['match_outcome'] == 0])
    draws = len(league_matches[league_matches['match_outcome'] == 1])
    away_wins = len(league_matches[league_matches['match_outcome'] == 2])
    
    avg_goals = (league_matches['home_team_goal'] + 
                league_matches['away_team_goal']).mean()
    
    # Team rankings by wins
    team_stats = {}
    for team in league_matches['home_team_long_name'].unique():
        home_games = league_matches[league_matches['home_team_long_name'] == team]
        away_games = league_matches[league_matches['away_team_long_name'] == team]
        
        wins = (len(home_games[home_games['match_outcome'] == 0]) + 
               len(away_games[away_games['match_outcome'] == 2]))
        
        team_stats[team] = wins
    
    # Display statistics
    logger.info(f"\nLeague Analysis: {league_name}")
    logger.info(f"Total Matches: {total_matches}")
    logger.info(f"Home Wins: {home_wins} ({home_wins/total_matches:.1%})")
    logger.info(f"Draws: {draws} ({draws/total_matches:.1%})")
    logger.info(f"Away Wins: {away_wins} ({away_wins/total_matches:.1%})")
    logger.info(f"Average Goals per Match: {avg_goals:.2f}")
    
    # Plot statistics
    os.makedirs('output/plots', exist_ok=True)
    
    # Outcome distribution
    plt.figure(figsize=(10, 6))
    outcomes = ['Home Wins', 'Draws', 'Away Wins']
    values = [home_wins, draws, away_wins]
    plt.bar(outcomes, values)
    plt.title(f'{league_name} Match Outcomes')
    plt.savefig(f'output/plots/{league_name.replace(" ", "_")}_outcomes.png')
    plt.close()
    
    # Team performance ranking
    plt.figure(figsize=(12, 6))
    teams = sorted(team_stats.items(), key=lambda x: x[1], reverse=True)
    plt.barh([t[0] for t in teams], [t[1] for t in teams])
    plt.title(f'{league_name} Team Wins')
    plt.tight_layout()
    plt.savefig(f'output/plots/{league_name.replace(" ", "_")}_teams.png')
    plt.close()
    
    # Generate plots
    base_path = create_league_plots(matches_df, league_name)
    
    logger.info(f"\nPlots have been saved in {base_path}*")
    logger.info("Generated plots:")
    logger.info("1. Goals Distribution")
    logger.info("2. Home vs Away Goals Comparison")
    logger.info("3. Team Performance Matrix")
    logger.info("4. Win Percentages")

def main():
    matches_df = load_match_data()
    leagues = sorted(matches_df['league_name'].unique())
    
    while True:
        logger.info("\nAvailable Leagues:")
        for i, league in enumerate(leagues, 1):
            logger.info(f"{i}. {league}")
        
        choice = input("\nSelect league number (0 to exit): ")
        if choice == '0':
            break
            
        try:
            idx = int(choice) - 1
            if 0 <= idx < len(leagues):
                analyze_league(matches_df, leagues[idx])
            else:
                logger.info("Invalid league number")
        except ValueError:
            logger.info("Please enter a valid number")

if __name__ == "__main__":
    main()