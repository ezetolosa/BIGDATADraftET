CONFIG = {
    'ROLLING_WINDOW_SIZE': 5,
    'MIN_MATCHES_REQUIRED': 3,
    'DATA_PATH': 'data/raw/match.csv',
    'FEATURE_COLUMNS': [
        'home_team_goal_rolling_avg',
        'away_team_goal_rolling_avg',
        'home_team_form',
        'away_team_form',
        'home_goal_diff_avg',
        'away_goal_diff_avg'
    ]
}