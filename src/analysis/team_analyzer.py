import pandas as pd

class TeamAnalyzer:
    def __init__(self, historical_data_path):
        self.data = pd.read_csv(historical_data_path)
    
    def analyze_team_performance(self, team, league, recent_matches=10):
        team_matches = self.data[
            ((self.data['home_team'] == team) | (self.data['away_team'] == team)) &
            (self.data['league'] == league)
        ].tail(recent_matches)
        
        # Calculate performance metrics
        goals_scored = []
        goals_conceded = []
        results = []
        
        for _, match in team_matches.iterrows():
            if match['home_team'] == team:
                goals_scored.append(match['home_goals'])
                goals_conceded.append(match['away_goals'])
                results.append('W' if match['home_goals'] > match['away_goals'] 
                             else 'D' if match['home_goals'] == match['away_goals'] 
                             else 'L')
            else:
                goals_scored.append(match['away_goals'])
                goals_conceded.append(match['home_goals'])
                results.append('W' if match['away_goals'] > match['home_goals'] 
                             else 'D' if match['away_goals'] == match['home_goals'] 
                             else 'L')
        
        return {
            'avg_goals_scored': sum(goals_scored) / len(goals_scored),
            'avg_goals_conceded': sum(goals_conceded) / len(goals_conceded),
            'form': ''.join(results),
            'wins': results.count('W'),
            'draws': results.count('D'),
            'losses': results.count('L')
        }