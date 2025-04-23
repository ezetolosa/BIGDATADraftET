# setup_kaggle.py – Setup Kaggle credentials and download soccer data
import os
import json
from dotenv import load_dotenv
import kaggle
from kaggle.api.kaggle_api_extended import KaggleApi
import zipfile

# Step 1: Load credentials from .env
load_dotenv()
KAGGLE_USERNAME = os.getenv("KAGGLE_USERNAME")
KAGGLE_KEY = os.getenv("KAGGLE_KEY")

if not KAGGLE_USERNAME or not KAGGLE_KEY:
    raise ValueError("Missing KAGGLE_USERNAME or KAGGLE_KEY in .env")

# Step 2: Create .kaggle directory and kaggle.json
kaggle_dir = os.path.join(os.path.expanduser("~"), ".kaggle")
os.makedirs(kaggle_dir, exist_ok=True)

kaggle_json_path = os.path.join(kaggle_dir, "kaggle.json")
with open(kaggle_json_path, "w") as f:
    json.dump({"username": KAGGLE_USERNAME, "key": KAGGLE_KEY}, f)

# Set appropriate permissions
os.chmod(kaggle_json_path, 0o600)
print("✅ Kaggle credentials configured")

# Step 3: Download dataset using Kaggle API
try:
    print("📥 Downloading dataset from Kaggle...")
    api = KaggleApi()
    api.authenticate()
    
    # Create data/raw directory
    os.makedirs("data/raw", exist_ok=True)
    
    # Download the dataset
    api.dataset_download_files(
        dataset="hugomathien/soccer",
        path="data/raw",
        unzip=True
    )
    print("✅ Dataset downloaded and unzipped in data/raw")
    
except Exception as e:
    print(f"❌ Error downloading dataset: {str(e)}")
    print("\nTry these steps:")
    print("1. Verify your Kaggle credentials")
    print("2. Make sure you have accepted the dataset rules on Kaggle website")
    print("3. Run this command manually in PowerShell as Administrator:")
    print("   kaggle datasets download -d hugomathien/soccer -p data/raw --unzip")
