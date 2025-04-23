from typing import List, Dict
import logging
import yaml

def setup_logging():
    """Configure logging for the project"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('soccer_analytics.log'),
            logging.StreamHandler()
        ]
    )

def validate_match_data(df) -> Dict[str, List]:
    """Validate match data quality"""
    issues = {
        "missing_values": [],
        "invalid_scores": [],
        "date_issues": []
    }
    
    # Check for missing values
    for col in df.columns:
        missing_count = df.filter(df[col].isNull()).count()
        if missing_count > 0:
            issues["missing_values"].append(f"{col}: {missing_count} missing values")
            
    return issues