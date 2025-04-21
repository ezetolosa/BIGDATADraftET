# ⚽ Big Data Soccer Analytics Project

This project uses PySpark to build an end-to-end pipeline for analyzing and predicting soccer match outcomes and performance KPIs based on historical data.

---

## 📦 Features

- Ingests data from Kaggle and optionally scrapes FBref
- Cleans and processes raw match data
- Extracts features like rolling goal averages, goals conceded, and form
- Trains two models:
  - Classification model to predict match outcomes (win/draw/loss)
  - Regression model to predict number of goals
- Interactive script for analyzing specific teams or leagues
- Outputs predictions with probabilities and goal estimates

---

## 🛠 How to Run

### 1. Clone the repo & install dependencies
```bash
git clone https://github.com/your-username/BigDataProject.git
cd BigDataProject
python -m venv venv
source venv/bin/activate  # or .\venv\Scripts\activate on Windows
pip install -r requirements.txt
```

### 2. Set up environment variables
Create a `.env` file based on `.env.example` for Kaggle/FBref keys.

### 3. Run the full pipeline
```bash
python main.py
```

### 4. Run the interactive analysis tool
```bash
python run_analysis.py
```
You can:
- Select a league and team
- Choose to analyze KPIs
- Predict outcomes or goals

---

## 📊 Example Output

| Home Team | Away Team | Win % | Draw % | Away Win % | Goals |
|-----------|-----------|--------|---------|-------------|--------|
| Barcelona | Sevilla   | 65.2%  | 18.7%   | 16.1%       | 2.1–1.3 |

---

## 📁 Project Structure

```
BigDataProject/
├── main.py                  # Runs the full ETL + model pipeline
├── run_analysis.py          # Interactive user analysis + prediction
├── src/
│   ├── data_processing.py
│   ├── feature_engineering.py
│   ├── model_training.py
│   └── fbref_ingest.py (optional)
├── models/                  # Saved trained models
├── data/
│   ├── raw/                 # Kaggle/raw files
│   └── processed/           # Feature-enriched data
├── output/                  # Predictions CSVs
├── logs/                    # Logs
├── .env.example             # Sample environment vars
├── requirements.txt         # Python dependencies
└── README.md
```

---

## 👥 Authors
- Ezequiel Tolosa
- Oluwadamilola Popoola

## 📘 License
MIT License

 
