import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_project_structure():
    """Create the initial project directory structure"""
    directories = [
        'data/raw',
        'data/processed',
        'output/predictions',
        'output/plots',
        'output/plots/league_analysis',
        'output/plots/form_analysis',
        'analysis/predictions',
        'analysis/team',
        'analysis/league',
        'analysis/form',
        'src',
        'logs',
        'models'
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        logger.info(f"Created directory: {directory}")

if __name__ == "__main__":
    create_project_structure()
    logger.info("Project structure setup complete!")