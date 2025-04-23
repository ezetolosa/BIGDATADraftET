# setup_kaggle.py – Setup Kaggle credentials and download soccer data
import os
import json
from dotenv import load_dotenv

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

os.chmod(kaggle_json_path, 0o600)
print("✅ Kaggle credentials configured")

# Step 3: Download dataset to data/raw
os.makedirs("data/raw", exist_ok=True)

print("📥 Downloading dataset from Kaggle...")
exit_code = os.system("kaggle datasets download -d hugomathien/soccer -p data/raw --unzip")

if exit_code == 0:
    print("✅ Dataset downloaded and unzipped in data/raw")
else:
    print("❌ Something went wrong. Try running the command manually:")
    print("kaggle datasets download -d hugomathien/soccer -p data/raw --unzip")
