# setup_structure.py – creates required folders for the project
import os

folders = [
    "data/raw",
    "data/processed",
    "models",
    "output",
    "logs"
]

def create_folders():
    for folder in folders:
        os.makedirs(folder, exist_ok=True)
        print(f"✅ Created folder: {folder}")

if __name__ == "__main__":
    create_folders()
