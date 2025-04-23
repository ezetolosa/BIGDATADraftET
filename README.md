# 🏆 Soccer Analytics & Prediction Platform

A comprehensive Big Data Analytics platform for soccer/football match predictions and team performance analysis using PySpark, Machine Learning, and real-time data processing.

## 🎯 Core Features

- **Match Outcome Prediction Engine**
  - Win/Draw/Loss probability calculation
  - Expected goals (xG) prediction
  - Head-to-head performance analysis

- **Team Performance Analytics**
  - Historical performance trends
  - Goal scoring patterns
  - Defensive effectiveness metrics

- **Interactive Analysis**
  - League selection
  - Team comparison tool
  - Custom match scenario simulation

## 📋 Installation & Setup

1. **Create Virtual Environment**
```powershell
python -m venv venv
.\venv\Scripts\activate
```

2. **Install Dependencies**
```powershell
pip install -r requirements.txt
```

3. **Kaggle Data Setup**
- Create a Kaggle account if you don't have one
- Download your Kaggle API credentials (`kaggle.json`)
- Place it in `%USERPROFILE%\.kaggle\`
- Run the setup script:
```powershell
python setup_kaggle.py
```

4. **Extract Data Tables**
- Run the extraction script to create CSV files:
```powershell
python extract_sqlite_to_csv.py
```
This will create the following CSV files in `data/raw/`:
- `match.csv`: Match results and statistics
- `team.csv`: Team information
- `league.csv`: League details
- `player.csv`: Player statistics

5. **Run Analysis Pipeline**
```powershell
python main.py
```

## 📊 Data Processing Flow

1. SQLite Database → CSV Files
2. CSV Loading & Cleaning
3. Feature Engineering
4. Model Training
5. Predictions & Analysis

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- Java 8+ (for PySpark)
- Kaggle account and API key

### Installation

1. **Clone & Setup Environment**
```powershell
git clone https://github.com/yourusername/soccer-analytics.git
cd soccer-analytics
python -m venv venv
.\venv\Scripts\activate
```

2. **Install Dependencies**
```powershell
pip install -r requirements.txt
```

3. **Configure Kaggle**
- Create a Kaggle account if you don't have one
- Go to your Kaggle account settings
- Click "Create New API Token"
- Download `kaggle.json` file
- Place `kaggle.json` in `%USERPROFILE%\.kaggle\` directory

4. **Download Dataset**
```powershell
python setup_kaggle.py
```
This will download the European Soccer Database from Kaggle and place it in the correct directory.

### Running the Analysis

1. **Data Pipeline Setup**
```powershell
python main.py            # Processes data and trains models
```

2. **Interactive Analysis**
```powershell
python run_analysis.py    # Starts interactive analysis tool
```

## 🚀 Getting Started

1. **Setup Environment**
```powershell
python -m venv venv
.\venv\Scripts\activate
pip install -r requirements.txt
```

2. **Configure Kaggle and Download Data**
- Place your `kaggle.json` in `%USERPROFILE%\.kaggle\`
- Run:
```powershell
python setup_kaggle.py  # Downloads database.sqlite
```

3. **Run Analysis Pipeline**
```powershell
python main.py  # Converts SQLite to CSV and processes data
```

4. **Interactive Analysis**
```powershell
python run_analysis.py
```

## 💻 Usage Examples

### Predict Match Outcome
```python
# Interactive prompt will ask for:
1. Select League (e.g., "Premier League")
2. Select Home Team
3. Select Away Team
4. Choose Analysis Type:
   - Match Prediction
   - Team Performance
   - Historical Analysis
```

### Sample Output
```
Match Prediction: Manchester City vs Liverpool
--------------------------------
Win Probability: 45%
Draw Probability: 28%
Loss Probability: 27%
Expected Goals: City 2.1 - 1.8 Liverpool
```

## 🗄️ Project Structure

```
soccer-analytics/
├── data/
│   ├── raw/              # Original dataset files
│   ├── processed/        # Cleaned and transformed data
│   └── features/         # Engineered features
├── src/
│   ├── pipeline/
│   │   ├── etl.py       # Data extraction and loading
│   │   └── features.py  # Feature engineering
│   ├── models/
│   │   ├── predictor.py # ML models
│   │   └── trainer.py   # Model training
│   └── utils/
│       ├── config.py    # Configuration
│       └── helpers.py   # Utility functions
├── notebooks/
│   └── analysis.ipynb   # EDA and visualizations
├── tests/
│   └── test_models.py   # Unit tests
├── main.py             # Pipeline orchestration
├── run_analysis.py     # Interactive analysis
└── requirements.txt
```

## 🔄 Complete Pipeline Steps

### 1. Initial Setup
```powershell
# Create and activate virtual environment
python -m venv venv
.\venv\Scripts\activate

# Install dependencies
pip install matplotlib seaborn pandas pyspark kaggle
```

### 2. Project Structure Setup
```powershell
# Create directory structure
python setup_structure.py
```
This creates:
- `data/raw`
- `data/processed`
- `output/predictions`
- `output/plots`
- `output/plots/league_analysis`

### 3. Data Download and Processing
```powershell
# Download Kaggle dataset
python setup_kaggle.py

# Convert SQLite to Parquet
python extract_sqlite.py

# Verify processing
python test_processing.py
```

### 4. Running Analysis Tools

#### Match Predictions
```powershell
python analysis/predictions/predict_local.py
```
- Select league
- Choose teams
- View head-to-head statistics
- Get match predictions

#### Team Analysis
```powershell
python analysis/team/soccer_insights.py
```
- Analyze single team performance
- Compare multiple teams
- View performance visualizations

#### League Statistics
```powershell
python analysis/league/league_analysis.py
```
- View league-wide statistics
- Team rankings
- Goal distributions

#### Form Analysis
```powershell
python analysis/form/form_analysis.py
```
- Track team form
- Historical performance
- Trend analysis

### 5. Generated Outputs

The analysis tools create visualizations in:
- `output/plots/` - Team analysis plots
- `output/plots/league_analysis/` - League statistics
- `output/predictions/` - Processed match data

### 6. Reset Pipeline
To start fresh while keeping code:
```powershell
# Remove generated data
rmdir /s /q "data\raw"
rmdir /s /q "data\processed"
rmdir /s /q "output\predictions"
rmdir /s /q "output\plots"

# Restart from step 2
```

## 📊 Data Sources

- Historical match data
- Player statistics
- Team performance metrics
- League standings
- Head-to-head records

## 🔧 Technical Stack

- **Data Processing**: PySpark, Pandas
- **Machine Learning**: Scikit-learn, PySpark ML
- **Visualization**: Seaborn, Matplotlib
- **Storage**: HDFS/Local Storage
- **API Integration**: RESTful APIs

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Commit changes
4. Push to the branch
5. Open a Pull Request

## 📝 License

MIT License - See LICENSE file for details

## 👥 Authors

- Your Name
- Contributors

## 📮 Contact

For questions or feedback, please open an issue or contact [your-email]


