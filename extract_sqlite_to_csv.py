# extract_sqlite_to_csv.py – Extract tables from database.sqlite to CSV and Parquet
import sqlite3
import pandas as pd
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_and_transform():
    """Extract data from SQLite and transform to CSV"""
    try:
        # Connect to SQLite
        logger.info("Connecting to SQLite database...")
        conn = sqlite3.connect('data/raw/database.sqlite')
        
        # Extract match data with joins
        logger.info("Extracting match data...")
        matches = pd.read_sql_query("""
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
        """, conn)
        
        # Add match outcome column
        matches['match_outcome'] = matches.apply(
            lambda row: 0 if row['home_team_goal'] > row['away_team_goal']
                       else 1 if row['home_team_goal'] == row['away_team_goal']
                       else 2, axis=1
        )
        
        # Create output directory
        os.makedirs('data/processed', exist_ok=True)
        
        # Save to CSV
        csv_path = 'data/processed/matches.csv'
        matches.to_csv(csv_path, index=False)
        logger.info(f"✅ CSV saved to {csv_path}")
        
        logger.info(f"✅ Successfully processed {len(matches)} matches")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error during extraction: {str(e)}")
        return False
        
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    success = extract_and_transform()
    if success:
        logger.info("✅ Data extraction complete")
    else:
        logger.error("❌ Data extraction failed")