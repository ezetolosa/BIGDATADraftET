import sqlite3
import pandas as pd
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def convert_database():
    """Convert SQLite database to CSV format"""
    try:
        # Connect to SQLite database
        conn = sqlite3.connect('data/raw/database.sqlite')
        
        # Execute query
        query = """
            SELECT 
                m.date,
                m.home_team_api_id,
                m.away_team_api_id,
                m.home_team_goal,
                m.away_team_goal,
                t1.team_long_name as home_team_long_name,
                t2.team_long_name as away_team_long_name,
                l.name as league_name
            FROM Match m
            JOIN Team t1 ON m.home_team_api_id = t1.team_api_id
            JOIN Team t2 ON m.away_team_api_id = t2.team_api_id
            JOIN League l ON m.league_id = l.id
        """
        
        df = pd.read_sql_query(query, conn)
        
        # Create processed directory if it doesn't exist
        os.makedirs('data/processed', exist_ok=True)
        
        # Save to CSV
        output_path = 'data/processed/matches.csv'
        df.to_csv(output_path, index=False)
        
        logger.info(f"✅ Data converted successfully to {output_path}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error converting database: {str(e)}")
        return False
        
    finally:
        if 'conn' in locals():
            conn.close()