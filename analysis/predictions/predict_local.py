import logging
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import numpy as np

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    return (SparkSession.builder
            .appName("Local Match Predictor")
            .config("spark.driver.memory", "1g")
            .config("spark.sql.shuffle.partitions", "2")
            .master("local[*]")
            .getOrCreate())

def display_leagues(matches_df):
    """Display available leagues and return mapping"""
    leagues = sorted(matches_df['league_name'].unique())
    logger.info("\nAvailable Leagues:")
    league_to_number = {}
    for i, league in enumerate(leagues, 1):
        logger.info(f"{i}. {league}")
        league_to_number[league] = i
    return league_to_number

def display_teams_in_league(matches_df, league):
    """Display teams in selected league and return mapping"""
    teams = sorted(matches_df[matches_df['league_name'] == league]['home_team_long_name'].unique())
    logger.info(f"\nTeams in {league}:")
    team_to_number = {}
    for i, team in enumerate(teams, 1):
        logger.info(f"{i}. {team}")
        team_to_number[team] = i
    return team_to_number

def get_filtered_matches(matches_df, team1, team2, venue):
    """Get matches filtered by teams and venue"""
    if venue == "1":  # team1 at home
        return matches_df[
            (matches_df['home_team_long_name'] == team1) & 
            (matches_df['away_team_long_name'] == team2)
        ]
    else:  # team1 away
        return matches_df[
            (matches_df['home_team_long_name'] == team2) & 
            (matches_df['away_team_long_name'] == team1)
        ]

def get_all_matches(matches_df, team1, team2):
    """Get all matches between two teams regardless of venue"""
    return matches_df[
        ((matches_df['home_team_long_name'] == team1) & (matches_df['away_team_long_name'] == team2)) |
        ((matches_df['home_team_long_name'] == team2) & (matches_df['away_team_long_name'] == team1))
    ]

def calculate_stats(filtered_matches, team1, team2, venue):
    """Calculate statistics from filtered matches"""
    total_matches = len(filtered_matches)
    
    if venue == "1":
        team1_wins = len(filtered_matches[filtered_matches['match_outcome'] == 0])
        draws = len(filtered_matches[filtered_matches['match_outcome'] == 1])
        team2_wins = len(filtered_matches[filtered_matches['match_outcome'] == 2])
        team1_goals_avg = filtered_matches['home_team_goal'].mean() if total_matches > 0 else 0
        team2_goals_avg = filtered_matches['away_team_goal'].mean() if total_matches > 0 else 0
    else:
        team1_wins = len(filtered_matches[filtered_matches['match_outcome'] == 2])
        draws = len(filtered_matches[filtered_matches['match_outcome'] == 1])
        team2_wins = len(filtered_matches[filtered_matches['match_outcome'] == 0])
        team1_goals_avg = filtered_matches['away_team_goal'].mean() if total_matches > 0 else 0
        team2_goals_avg = filtered_matches['home_team_goal'].mean() if total_matches > 0 else 0

    return {
        'total_matches': total_matches,
        'team1_wins': team1_wins,
        'draws': draws,
        'team2_wins': team2_wins,
        'team1_goals_avg': team1_goals_avg,
        'team2_goals_avg': team2_goals_avg
    }

def calculate_probabilities(stats, matches_df, team1, team2, venue):
    """Calculate match probabilities with corrected away performance logic"""
    venue_games = stats['total_matches']
    
    if venue_games == 0:
        return [0.35, 0.30, 0.35]  # Default probabilities with no history
    
    # Calculate venue-specific probabilities
    if venue == "1":  # team1 at home
        if stats['team1_wins'] == venue_games:  # Perfect home record
            return [0.70, 0.20, 0.10]
        elif stats['team2_wins'] == venue_games:  # Perfect away record
            return [0.15, 0.25, 0.60]
        
        # Normal case for home games
        home_ratio = stats['team1_wins'] / venue_games
        draw_ratio = stats['draws'] / venue_games
        away_ratio = stats['team2_wins'] / venue_games
        
    else:  # team1 away
        if stats['team2_wins'] == venue_games:  # Home team perfect record
            return [0.15, 0.25, 0.60]
        elif stats['team1_wins'] == venue_games:  # Away team perfect record
            return [0.60, 0.25, 0.15]
        
        # Normal case for away games - team1 is away team
        home_ratio = stats['team2_wins'] / venue_games
        draw_ratio = stats['draws'] / venue_games
        away_ratio = stats['team1_wins'] / venue_games
    
    # Add minimum probability and weight adjustments
    min_prob = 0.10
    home_prob = max(home_ratio * 0.7, min_prob)
    draw_prob = max(draw_ratio * 0.7 + 0.15, min_prob)  # Slight boost to draws
    away_prob = max(away_ratio * 0.7, min_prob)
    
    # Normalize probabilities
    total = home_prob + draw_prob + away_prob
    return [
        home_prob / total,
        draw_prob / total,
        away_prob / total
    ]

def display_prediction(team1, team2, stats, matches_df, venue):
    """Display match statistics and prediction with consistent team ordering"""
    probabilities = calculate_probabilities(stats, matches_df, team1, team2, venue)
    
    if venue == "1":  # team1 at home
        home_team = team1
        away_team = team2
        home_wins = stats['team1_wins']
        away_wins = stats['team2_wins']
        win_prob = probabilities[0]
        lose_prob = probabilities[2]
    else:  # team1 away - swap presentation order
        home_team = team2
        away_team = team1
        home_wins = stats['team2_wins']
        away_wins = stats['team1_wins']
        win_prob = probabilities[2]  # Swap probabilities for display
        lose_prob = probabilities[0]
    
    # Display header with consistent home vs away format
    logger.info(f"\nHead-to-Head Statistics ({home_team} vs {away_team}):")
    logger.info(f"Matches at this venue: {stats['total_matches']}")
    logger.info(f"{home_team} wins: {home_wins}")
    logger.info(f"Draws: {stats['draws']}")
    logger.info(f"{away_team} wins: {away_wins}")
    
    # Display probabilities in home vs away format
    logger.info("\nPrediction Probabilities:")
    logger.info(f"{home_team} win: {win_prob:.2%}")
    logger.info(f"Draw: {probabilities[1]:.2%}")
    logger.info(f"{away_team} win: {lose_prob:.2%}")
    
    # Determine winner based on highest probability
    if win_prob > max(probabilities[1], lose_prob):
        outcome = f"{home_team} Win"
    elif lose_prob > max(probabilities[1], win_prob):
        outcome = f"{away_team} Win"
    else:
        outcome = "Draw"
    
    logger.info(f"\nPredicted Outcome: {outcome}")

def menu():
    """Main menu function with improved navigation"""
    spark = create_spark_session()
    matches_df = spark.read.parquet("output/predictions").toPandas()
    
    while True:
        # Display leagues
        league_to_number = display_leagues(matches_df)
        
        # Select league
        league_choice = input("\nSelect league (number) or 0 to exit: ")
        if league_choice == '0':
            break
            
        try:
            league_idx = int(league_choice)
            if 1 <= league_idx <= len(league_to_number):
                selected_league = [league for league, num in league_to_number.items() 
                                 if num == league_idx][0]
                
                while True:  # League loop
                    # Display teams in selected league
                    team_to_number = display_teams_in_league(matches_df, selected_league)
                    
                    # Select first team
                    team1_choice = input("\nSelect first team (number), 'b' for leagues, or 'x' to exit: ")
                    if team1_choice.lower() == 'b':
                        break  # Go back to league selection
                    if team1_choice.lower() == 'x':
                        return
                    
                    try:
                        team1_idx = int(team1_choice)
                        if 1 <= team1_idx <= len(team_to_number):
                            team1 = [team for team, num in team_to_number.items() 
                                   if num == team1_idx][0]
                            
                            while True:  # First team loop
                                # Display opponent options
                                logger.info(f"\nSelect opponent for {team1}:")
                                for i, team in enumerate(team_to_number.keys(), 1):
                                    if team != team1:
                                        logger.info(f"{i}. {team}")
                                
                                team2_choice = input("\nSelect opponent (number), 'b' to change team, 'l' for leagues, or 'x' to exit: ")
                                if team2_choice.lower() == 'b':
                                    break  # Go back to team selection
                                if team2_choice.lower() == 'l':
                                    break  # Will break twice to go to league selection
                                if team2_choice.lower() == 'x':
                                    return
                                
                                try:
                                    team2_idx = int(team2_choice)
                                    team2 = [team for team, num in team_to_number.items() 
                                           if num == team2_idx][0]
                                    
                                    if team2 == team1:
                                        logger.info("Cannot select same team. Please choose different opponent.")
                                        continue
                                    
                                    while True:  # Match options loop
                                        venue = input("\nWhere is the match?\n1. First team home\n2. First team away\n"
                                                    "3. Change opponent\n4. Change team\n5. Change league\n6. Exit\nChoice: ")
                                        
                                        if venue in ['1', '2']:
                                            filtered_matches = get_filtered_matches(matches_df, team1, team2, venue)
                                            stats = calculate_stats(filtered_matches, team1, team2, venue)
                                            display_prediction(team1, team2, stats, matches_df, venue)
                                            
                                            next_action = input("\n1. Try different venue\n2. Change opponent\n"
                                                              "3. Change team\n4. Change league\n5. Exit\nChoice: ")
                                            
                                            if next_action == '2':
                                                break  # Go back to opponent selection
                                            elif next_action == '3':
                                                break  # Will break twice to go to team selection
                                            elif next_action == '4':
                                                break  # Will break three times to go to league selection
                                            elif next_action == '5':
                                                return
                                                
                                        elif venue == '3':
                                            break  # Go back to opponent selection
                                        elif venue == '4':
                                            break  # Will break twice to go to team selection
                                        elif venue == '5':
                                            break  # Will break three times to go to league selection
                                        elif venue == '6':
                                            return
                                        
                                    # Handle multiple breaks for team/league changes
                                    if venue == '4' or next_action == '3':
                                        break  # Break again to change team
                                    if venue == '5' or next_action == '4':
                                        break  # Will break again to change league
                                        
                                except ValueError:
                                    logger.info("Please enter a valid number.")
                                    
                            # Handle league change
                            if venue == '5' or next_action == '4':
                                break  # Break again to change league
                                    
                    except ValueError:
                        logger.info("Please enter a valid number.")
                        
        except ValueError:
            logger.info("Please enter a valid number.")

if __name__ == "__main__":
    menu()