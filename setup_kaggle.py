# setup_kaggle.py – Setup Kaggle credentials and download soccer data
import os
import json
import subprocess
from pathlib import Path
import sys

def setup_kaggle_credentials():
    """Set up Kaggle API credentials"""
    # Get the actual home directory path
    home = os.path.expanduser('~')
    kaggle_dir = os.path.join(home, '.kaggle')
    kaggle_json = os.path.join(kaggle_dir, 'kaggle.json')
    
    print(f"Setting up Kaggle credentials in: {kaggle_dir}")
    
    try:
        # Create .kaggle directory if it doesn't exist
        os.makedirs(kaggle_dir, exist_ok=True)
        
        if not os.path.exists(kaggle_json):
            print("Please get your Kaggle API credentials from:")
            print("https://www.kaggle.com/account -> 'Create New API Token'")
            credentials = {
                "username": input("Enter your Kaggle username: ").strip(),
                "key": input("Enter your Kaggle API key: ").strip()
            }
            
            # Write credentials file
            with open(kaggle_json, 'w') as f:
                json.dump(credentials, f)
            
            # Set file permissions
            os.chmod(kaggle_json, 0o600)
            print(f"✅ Credentials saved to: {kaggle_json}")
        else:
            print("✅ Kaggle credentials file already exists")
            
    except Exception as e:
        print(f"❌ Error setting up credentials: {str(e)}")
        return False
        
    return True

def download_dataset():
    """Download dataset from Kaggle"""
    try:
        # Create data directory structure
        os.makedirs("data/raw", exist_ok=True)
        
        print("📥 Downloading dataset from Kaggle...")
        
        # Try multiple potential kaggle executable locations
        possible_paths = [
            os.path.join(sys.prefix, 'Scripts', 'kaggle.exe'),  # venv path
            os.path.expanduser('~\\AppData\\Roaming\\Python\\Python313\\Scripts\\kaggle.exe'),  # user install path
            'kaggle'  # system PATH
        ]
        
        kaggle_cmd = None
        for path in possible_paths:
            if os.path.exists(path):
                kaggle_cmd = path
                break
        
        if not kaggle_cmd:
            raise FileNotFoundError("Kaggle executable not found. Please ensure Kaggle is installed.")

        # Run kaggle command with direct path
        result = subprocess.run(
            [kaggle_cmd, "datasets", "download", 
             "-d", "hugomathien/soccer",
             "-p", "data/raw", 
             "--unzip"],
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            print(f"❌ Error downloading dataset:")
            print(f"stdout: {result.stdout}")
            print(f"stderr: {result.stderr}")
            return False
            
        print("✅ Dataset downloaded and unzipped in data/raw")
        return True
        
    except Exception as e:
        print(f"❌ Unexpected error: {str(e)}")
        if hasattr(e, 'stdout'):
            print(f"stdout: {e.stdout}")
        if hasattr(e, 'stderr'):
            print(f"stderr: {e.stderr}")
        return False

if __name__ == "__main__":
    setup_kaggle_credentials()
    download_dataset()
