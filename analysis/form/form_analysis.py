import logging
import pandas as pd
from pyspark.sql import SparkSession
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    return (SparkSession.builder
            .appName("Form Analysis")
            .config("spark.driver.memory", "1g")
            .master("local[*]")
            .getOrCreate())

def calculate_form(matches_df, team_name, window_size=5):
    """Calculate rolling form for a team"""
    team_matches = matches_df[
        (matches_df['home_team_long_name'] == team_name) | 
        (matches_df['away_team_long_name'] == team_name)
    ].sort_values('date')
    
    results = []
    for _, match in team_matches.iterrows():
        if match['home_team_long_name'] == team_name:
            # Points for home games
            points = 3 if match['match_outcome'] == 0 else 1 if match['match_outcome'] == 1 else 0
            goals_scored = match['home_team_goal']
            goals_conceded = match['away_team_goal']
        else:
            # Points for away games
            points = 3 if match['match_outcome'] == 2 else 1 if match['match_outcome'] == 1 else 0
            goals_scored = match['away_team_goal']
            goals_conceded = match['home_team_goal']
            
        results.append({
            'date': match['date'],
            'points': points,
            'goals_scored': goals_scored,
            'goals_conceded': goals_conceded
        })
    
    form_df = pd.DataFrame(results)
    form_df['rolling_points'] = form_df['points'].rolling(window=window_size, min_periods=1).mean()
    form_df['rolling_goals_scored'] = form_df['goals_scored'].rolling(window=window_size, min_periods=1).mean()
    form_df['rolling_goals_conceded'] = form_df['goals_conceded'].rolling(window=window_size, min_periods=1).mean()
    
    return form_df

def plot_form_analysis(form_df, team_name, output_dir):
    """Create visualization of team form"""
    plt.figure(figsize=(15, 10))
    
    # Plot rolling averages
    plt.subplot(3, 1, 1)
    plt.plot(form_df['date'], form_df['rolling_points'], 'b-', label='Points per Game')
    plt.title(f'{team_name} Form Analysis')
    plt.ylabel('Avg Points (last 5 games)')
    plt.legend()
    
    plt.subplot(3, 1, 2)
    plt.plot(form_df['date'], form_df['rolling_goals_scored'], 'g-', label='Goals Scored')
    plt.plot(form_df['date'], form_df['rolling_goals_conceded'], 'r-', label='Goals Conceded')
    plt.ylabel('Avg Goals (last 5 games)')
    plt.legend()
    
    # Add form indicators
    plt.subplot(3, 1, 3)
    plt.scatter(form_df['date'], form_df['points'], 
               c=['green' if x == 3 else 'yellow' if x == 1 else 'red' for x in form_df['points']])
    plt.ylabel('Match Results\n(W/D/L)')
    
    plt.tight_layout()
    plt.savefig(f'{output_dir}/{team_name.replace(" ", "_")}_form.png')
    plt.close()

def display_leagues(matches_df):
    """Display available leagues and return mapping"""
    leagues = sorted(matches_df['league_name'].unique())
    logger.info("\nAvailable Leagues:")
    for i, league in enumerate(leagues, 1):
        logger.info(f"{i}. {league}")
    return {i: league for i, league in enumerate(leagues, 1)}

def display_teams_in_league(matches_df, league_name):
    """Display teams in selected league and return mapping"""
    teams = sorted(matches_df[matches_df['league_name'] == league_name]['home_team_long_name'].unique())
    logger.info(f"\nTeams in {league_name}:")
    for i, team in enumerate(teams, 1):
        logger.info(f"{i}. {team}")
    return {i: team for i, team in enumerate(teams, 1)}

def main():
    spark = create_spark_session()
    matches_df = spark.read.parquet("output/predictions").toPandas()
    
    # Create output directory
    output_dir = 'output/plots/form_analysis'
    os.makedirs(output_dir, exist_ok=True)
    
    while True:
        # First, select league
        league_mapping = display_leagues(matches_df)
        league_choice = input("\nSelect league number (0 to exit): ")
        
        if league_choice == '0':
            break
            
        try:
            league_idx = int(league_choice)
            if league_idx in league_mapping:
                selected_league = league_mapping[league_idx]
                
                while True:
                    # Then select team from chosen league
                    team_mapping = display_teams_in_league(matches_df, selected_league)
                    team_choice = input("\nSelect team number (0 for leagues, 'x' to exit): ")
                    
                    if team_choice == '0':
                        break
                    if team_choice.lower() == 'x':
                        return
                        
                    try:
                        team_idx = int(team_choice)
                        if team_idx in team_mapping:
                            team_name = team_mapping[team_idx]
                            form_df = calculate_form(matches_df, team_name)
                            plot_form_analysis(form_df, team_name, output_dir)
                            
                            logger.info(f"\nForm analysis for {team_name}:")
                            logger.info(f"Total matches analyzed: {len(form_df)}")
                            logger.info(f"Average points per game: {form_df['points'].mean():.2f}")
                            logger.info(f"Plot saved as: {output_dir}/{team_name.replace(' ', '_')}_form.png")
                            
                            input("\nPress Enter to continue...")
                        else:
                            logger.info("Invalid team number")
                    except ValueError:
                        logger.info("Please enter a valid number")
            else:
                logger.info("Invalid league number")
        except ValueError:
            logger.info("Please enter a valid number")

if __name__ == "__main__":
    main()