# 🏆 Soccer Analytics & Prediction Platform

# Big Data Analytics in Soccer: Performance Insights and Predictive Modeling

**Authors:**
- Oluwadamilola Popoola
- Ezequiel Tolosa

## 📖 About
This project leverages big data technologies to analyze European soccer matches and predict outcomes using machine learning. Built with PySpark and SQLite, it processes over 25,000 matches from top leagues to provide insights into team performance, historical trends, and match predictions. The platform combines historical data analysis with predictive modeling to offer comprehensive soccer analytics tools.

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
python -m pip install -r requirements.txt
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

4. **Run Analysis**
```powershell
python run_analysis.py
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

## 🔄 Running Analysis Tools

**Match Predictions**
```powershell
python analysis/predictions/predict_local.py
```
- Select league
- Choose teams
- View head-to-head statistics
- Get match predictions

**Title Chances Prediction**
```powershell
python analysis/predictions/predict_title_chances.py
```
- Predict league title chances
- Based on 3-year historical data (2014-2016)
- Uses head-to-head statistics
- Multiple season simulations
- Interactive league selection

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

**Standings Prediction**
```powershell
python analysis/predictions/predict_standings.py
```
- Predict hypothetical season standings
- Based on 3-year historical data
- Home/away performance analysis
- Professional visualizations

**Form Analysis**
```powershell
python analysis/form/form_analysis.py
```
- Track team form
- Historical performance
- Trend analysis

## 📊 Generated Outputs

The analysis tools create visualizations in:
- `output/plots/team_insights/` - Team performance analysis
- `output/plots/league_analysis/` - League statistics, predicted standings and title winners.
- `output/plots/form_analysis/` - Form tracking visualizations
- `output/predictions/` - Match prediction data

Each visualization includes:
- Professional color schemes
- Clear data presentation
- Detailed statistics
- High-resolution output
- Print-ready formatting

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

## 🌍 Available Leagues

The dataset covers 11 major European leagues:
1. Belgium Jupiler League
2. England Premier League
3. France Ligue 1
4. Germany 1. Bundesliga
5. Italy Serie A
6. Netherlands Eredivisie
7. Poland Ekstraklasa
8. Portugal Liga ZON Sagres
9. Scotland Premier League
10. Spain LIGA BBVA
11. Switzerland Super League

## 📊 Data Distribution

### League Statistics (2008-2016)

| League Name | Matches | Seasons |
|------------|---------|---------|
| France Ligue 1 | 3,040 | 8 |
| England Premier League | 3,040 | 8 |
| Spain LIGA BBVA | 3,040 | 8 |
| Italy Serie A | 3,017 | 8 |
| Netherlands Eredivisie | 2,448 | 8 |
| Germany 1. Bundesliga | 2,448 | 8 |
| Portugal Liga ZON Sagres | 2,052 | 7 |
| Poland Ekstraklasa | 1,920 | 8 |
| Scotland Premier League | 1,824 | 8 |
| Belgium Jupiler League | 1,728 | 6 |
| Switzerland Super League | 1,422 | 6 |
**Total Matches: 25,979**

### Data Quality Metrics
- Complete match records: 25,979
- Time period: 2008-2016 (8 seasons)
- Leagues covered: 11 major European leagues
- Teams analyzed: ~300 unique teams
- Data completeness: 100% for core metrics
  - Match dates
  - Team identifiers
  - Goals scored
  - League information

## 💡 Example Prediction Interactions

### Match Prediction Example
```plaintext
# Sample Match Prediction Session

Available Leagues:
1. Italy Serie A
[User selects Serie A]

Teams in Italy Serie A:
- Milan
- Inter
- Juventus
- Roma
[and 28 more teams...]

[User selects Milan vs Inter]

Head-to-Head Statistics (Milan vs Inter):
Matches at this venue: 2
Milan wins: 1
Draws: 0
Inter wins: 1

Prediction Probabilities:
Milan win: 41.18%
Draw: 17.65%
Inter win: 41.18%

Predicted Outcome: Draw
```

### Title Chances Prediction Example
```plaintext
=== Title Prediction Menu ===
1. Select League
2. Exit
Choice (1-2): 1

Available leagues:
1. Belgium Jupiler League
2. France Ligue 1
3. Switzerland Super League
4. Poland Ekstraklasa
5. England Premier League
6. Portugal Liga ZON Sagres
7. Spain LIGA BBVA
8. Italy Serie A
9. Scotland Premier League
10. Netherlands Eredivisie
11. Germany 1. Bundesliga

Select league number (or 'b' for main menu): 5

Loading and processing data...
Calculating team metrics...
Running 500 simulations...
Creating visualization...

Predicted Title Chances for England Premier League:
Based on historical performance 2014-2016
--------------------------------------------------
Manchester City                28.2%
Arsenal                        19.0%
Chelsea                        18.2%
Liverpool                      11.2%
Tottenham Hotspur              8.0%
Manchester United              6.2%
Leicester City                 4.0%
Southampton                    1.6%
West Ham United                1.0%
Everton                        1.0%
[Additional teams below 1%...]

Press Enter to continue...

=== Title Prediction Menu ===
1. Select League
2. Exit
Choice (1-2): 2
```

### Standings Prediction Example
```plaintext
Available leagues:
1. Belgium Jupiler League
2. France Ligue 1
3. Switzerland Super League
4. Poland Ekstraklasa
5. England Premier League
6. Portugal Liga ZON Sagres
7. Spain LIGA BBVA
8. Italy Serie A
9. Scotland Premier League
10. Netherlands Eredivisie
11. Germany 1. Bundesliga

Select league number: 6

Loading match data...
Processing data for analysis...
Analyzing matches from 2013-05-15 to 2016-05-15

Predicted Portugal Liga ZON Sagres Standings:
1. SL Benfica                | P:42.0 | W:29.0 D:6.0 L:7.0 | GF:111.0 GA:50.0 GD:61.0 | Pts:93
2. Sporting CP               | P:42.0 | W:28.0 D:7.0 L:7.0 | GF:104.0 GA:56.0 GD:48.0 | Pts:91
3. Estoril Praia            | P:42.0 | W:21.0 D:13.0 L:8.0 | GF:71.0 GA:49.0 GD:22.0 | Pts:76
4. FC Porto                 | P:42.0 | W:21.0 D:10.0 L:11.0 | GF:73.0 GA:47.0 GD:26.0 | Pts:73
5. Rio Ave FC               | P:42.0 | W:20.0 D:9.0 L:13.0 | GF:51.0 GA:44.0 GD:7.0 | Pts:69
[...Additional teams...]

Key Statistics:
- Games Played (P)
- Wins (W), Draws (D), Losses (L)
- Goals For (GF), Goals Against (GA)
- Goal Difference (GD)
- Total Points (Pts)

Output saved as: output/plots/league_analysis/predicted_standings_Portugal_Liga_ZON_Sagres.png
```

### League Analysis Example
```plaintext
Available Leagues:
1. Belgium Jupiler League
2. England Premier League
3. France Ligue 1
4. Germany 1. Bundesliga
5. Italy Serie A
6. Netherlands Eredivisie
7. Poland Ekstraklasa
8. Portugal Liga ZON Sagres
9. Scotland Premier League
10. Spain LIGA BBVA
11. Switzerland Super League

Select league number (0 to exit): 3

League Analysis: France Ligue 1
Total Matches: 887
Home Wins: 376 (42.4%)
Draws: 256 (28.9%)
Away Wins: 255 (28.7%)
Average Goals per Match: 2.43

Generated plots:
1. Goals Distribution
2. Home vs Away Goals Comparison
3. Team Performance Matrix
4. Win Percentages

Output saved to: output/plots/league_analysis/France_Ligue_1_analysis.png
```

### Form Analysis Example
```plaintext
Available Leagues:
1. Belgium Jupiler League
2. England Premier League
3. France Ligue 1
4. Germany 1. Bundesliga
5. Italy Serie A
6. Netherlands Eredivisie
7. Poland Ekstraklasa
8. Portugal Liga ZON Sagres
9. Scotland Premier League
10. Spain LIGA BBVA
11. Switzerland Super League

Select league number (0 to exit): 7

Teams in Poland Ekstraklasa:
1. Arka Gdynia
2. Cracovia
3. GKS Bełchatów
4. Górnik Łęczna
5. Jagiellonia Białystok
6. Korona Kielce
7. Lech Poznań
8. Lechia Gdańsk
9. Legia Warszawa
10. Odra Wodzisław
[...Additional teams...]

Select team number (0 for leagues, 'x' to exit): 9

Form analysis for Legia Warszawa:
Total matches analyzed: 76
Average points per game: 1.74
Plot saved as: output/plots/form_analysis/Legia_Warszawa_form.png

Key Performance Indicators:
- Historical match performance
- Points per game average
- Form trend visualization
- Season-by-season comparison
```

### Soccer Insights Example
```plaintext
Soccer Insights Menu:
1. Analyze Single Team
2. Compare Two Teams
3. Exit

Select option: 1

Available Leagues and Teams:
Belgium Jupiler League:
1. Beerschot AC
2. Club Brugge KV
3. FCV Dender EH
[...Additional teams...]

Select team number: 270

Team Analysis: Real Madrid CF
Total Matches: 90

Home Performance:
Wins: 41, Draws: 3, Losses: 2
Home Win Rate: 89.13%

Away Performance:
Wins: 28, Draws: 7, Losses: 9
Away Win Rate: 63.64%

Overall Win Rate: 76.67%

Plot saved as: output/plots/team_insights/Real_Madrid_CF_performance.png

Key Performance Metrics:
- Home/Away performance breakdown
- Win rate analysis
- Match history visualization
- Goal scoring patterns
- Performance trends
```

### Interactive Features
- League selection from 11 major European leagues
- Historical data analysis (2014-2016)
- Multiple season simulations
- Percentage-based predictions
- Professional visualizations
- Interactive menu system

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
python run_analysis.py
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

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

### Dataset License
The European Soccer Database is available on Kaggle under [CC0: Public Domain](https://creativecommons.org/publicdomain/zero/1.0/).

## 📧 Contact

- **Oluwadamilola Popoola** - S536877@nwmissouri.edu
- **Ezequiel Tolosa** - S556421@nwmissouri.edu

Project Link: [https://github.com/yourusername/BIGDATADraftET](https://github.com/yourusername/BIGDATAProject)


