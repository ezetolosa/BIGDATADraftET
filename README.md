# 🏆 Soccer Analytics & Prediction Platform

# Big Data Analytics in Soccer: Performance Insights and Predictive Modeling

**Authors:**
- Oluwadamilola Popoola
- Ezequiel Tolosa

A Big Data Analytics platform for soccer match predictions and team performance analysis.

## 🎯 Core Features

- **Match Data Processing**
  - SQLite to CSV conversion
  - Parquet data transformation
  - Performance metrics calculation

- **Team Analysis**
  - Historical match data
  - Win/loss statistics
  - Goal scoring patterns

## 📋 Requirements

### System Requirements
- Python 3.8+
- Java 8+ (for PySpark)
- Windows 10 or higher
- 8GB RAM minimum
- Internet connection

### Python Packages
```powershell
pyspark>=3.5.0
pandas>=2.2.0
scikit-learn>=1.4.0
numpy>=1.24.0
matplotlib>=3.8.0
seaborn>=0.13.0
python-dotenv>=1.0.0
kaggle>=1.6.0
jupyter>=1.0.0
pyyaml>=6.0.1
```

## 🚀 Setup & Installation

1. **Create Virtual Environment**
```powershell
python -m venv venv
.\venv\Scripts\activate
```

2. **Install Dependencies**
```powershell
pip install -r requirements.txt
```

3. **Kaggle Configuration**
- Create `.env` file with your Kaggle credentials:
```plaintext
KAGGLE_USERNAME=your_username
KAGGLE_KEY=your_api_key
```

4. **Create Project Structure**
```powershell
python setup_structure.py
```

Creates:
```
project_root/
├── data/
│   ├── raw/             # SQLite database
│   └── processed/       # CSV files
├── output/
│   ├── predictions/     # Parquet files
│   └── plots/          # Visualizations
├── src/
│   ├── analysis/       
│   └── utils/          
├── logs/               
└── models/             
```

## 📊 Data Pipeline

1. **Download Dataset**
```powershell
python setup_kaggle.py
```

2. **Convert SQLite to CSV**
```powershell
python extract_sqlite_to_csv.py
```

3. **Process Data**
```powershell
python main.py
```

## 📊 Data Sources

### Kaggle European Soccer Database
- Historical match data spanning multiple seasons
- Comprehensive team and player statistics
- League-specific performance metrics
- Over 25,000 matches from major European leagues
- Source: [European Soccer Database](https://www.kaggle.com/datasets/hugomathien/soccer/data)

Database includes:
- Match results and statistics
- Team performance metrics
- League classifications
- Seasonal data from 2008 to 2016
- Coverage of major European leagues

4. **Running Analysis Tools**

**Match Predictions**
```powershell
python analysis/predictions/predict_local.py
```
- Select league
- Choose teams
- View head-to-head statistics
- Get match predictions

**Team Analysis**
```powershell
python analysis/team/soccer_insights.py
```
- Analyze single team performance
- Compare multiple teams
- View performance visualizations

**League Statistics**
```powershell
python analysis/league/league_analysis.py
```
- View league-wide statistics
- Team rankings
- Goal distributions

**Form Analysis**
```powershell
python analysis/form/form_analysis.py
```
- Track team form
- Historical performance
- Trend analysis

5. **Generated Outputs**

The analysis tools create visualizations in:
- `output/plots/` - Team analysis plots
- `output/plots/league_analysis/` - League statistics
- `output/predictions/` - Processed match data

## 📚 Data Dictionary

### Integrated Dataset Columns

| Column Name | Description | Type |
|------------|-------------|------|
| date | Match date | DATETIME |
| home_team_api_id | Unique identifier for home team | INTEGER |
| away_team_api_id | Unique identifier for away team | INTEGER |
| home_team_goal | Goals scored by home team | INTEGER |
| away_team_goal | Goals scored by away team | INTEGER |
| home_team_long_name | Full name of home team | STRING |
| away_team_long_name | Full name of away team | STRING |
| league_name | Name of the league | STRING |
| match_outcome | Result (0=home win, 1=draw, 2=away win) | INTEGER |

### Derived Features
| Feature Name | Description | Type |
|-------------|-------------|------|
| goal_difference | Goal difference (home - away) | INTEGER |
| total_goals | Total goals in match | INTEGER |
| points_home | Points earned by home team | INTEGER |
| points_away | Points earned by away team | INTEGER |
| form_home | Recent form of home team (last 5 matches) | FLOAT |
| form_away | Recent form of away team (last 5 matches) | FLOAT |

## 🔄 Reset Pipeline

To start fresh while keeping code:
```powershell
# Remove generated data
rmdir /s /q "data\raw"
rmdir /s /q "data\processed"
rmdir /s /q "output\predictions"
rmdir /s /q "output\plots"

# Restart pipeline
python setup_structure.py
python setup_kaggle.py
python extract_sqlite_to_csv.py
python main.py
```

## 🔧 Technical Stack

- **Data Processing**: PySpark, Pandas
- **Storage**: SQLite, Parquet
- **Analysis**: Python
- **Visualization**: Matplotlib, Seaborn

## 📝 Project Structure

Key files:
- `setup_structure.py`: Creates directory structure
- `setup_kaggle.py`: Downloads dataset
- `extract_sqlite_to_csv.py`: Converts database to CSV
- `main.py`: Processes data
- `test_processing.py`: Validates pipeline

## ⚠️ Common Issues

1. **Python Worker Connection**
   - Ensure Java is installed
   - Check Python environment
   - Verify PySpark configuration

2. **Kaggle Authentication**
   - Verify credentials in `.env`
   - Accept dataset terms on Kaggle website

3. **File Permissions**
   - Run VS Code as administrator if needed
   - Check write permissions in output directories


