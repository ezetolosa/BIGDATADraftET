import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import os
import logging

logger = logging.getLogger(__name__)

def plot_team_performance(results_df, team_name):
    """Create visualizations for team performance analysis"""
    try:
        # Create output directory if it doesn't exist
        output_dir = 'output/plots'
        os.makedirs(output_dir, exist_ok=True)
        
        # Create figure with subplots
        plt.figure(figsize=(15, 10))
        
        # Plot 1: Win/Draw/Loss distribution
        plt.subplot(2, 2, 1)
        outcome_counts = results_df['match_outcome'].value_counts()
        plt.pie(outcome_counts, 
                labels=['Win', 'Draw', 'Loss'], 
                colors=['green', 'yellow', 'red'],
                autopct='%1.1f%%')
        plt.title(f'{team_name} Match Outcomes')
        
        # Plot 2: Goals scored vs conceded
        plt.subplot(2, 2, 2)
        goals_data = pd.DataFrame({
            'Scored': results_df['home_team_goal'],
            'Conceded': results_df['away_team_goal']
        })
        goals_data.plot(kind='box')
        plt.title('Goals Distribution')
        
        # Save plot
        output_path = f'{output_dir}/{team_name.replace(" ", "_")}_analysis.png'
        plt.tight_layout()
        plt.savefig(output_path)
        plt.close()
        
        logger.info(f"✅ Plots saved to {output_path}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error creating plots: {str(e)}")
        return False