import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd

def plot_team_performance(team_stats: pd.DataFrame, team_name: str):
    """Create visualization of team performance"""
    plt.figure(figsize=(12, 6))
    
    # Goals comparison
    sns.barplot(data=team_stats, x=['Goals Scored', 'Goals Conceded'], 
                y=[team_stats['avg_goals_scored'].iloc[0], 
                   team_stats['avg_goals_conceded'].iloc[0]])
    
    plt.title(f"{team_name} Performance Analysis")
    plt.tight_layout()
    plt.savefig(f"results/{team_name.lower().replace(' ', '_')}_performance.png")