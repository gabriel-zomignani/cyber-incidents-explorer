from pathlib import Path
import pandas as pd

DATA = Path("data/cyber_events_2014_2025.csv")
OUT_DIR = Path("outputs")
OUT_DIR.mkdir(exist_ok=True)
OUT_FILE = OUT_DIR / "cyber_clean.csv"

def main():
    #load csv 
    if not DATA.exists():
        print(f"csv not found at {DATA}")
        return
    df = pd.read_csv(DATA, low_memory=False)
    print(f"loaded: {df.shape[0]} rows x {df.shape[1]} columns")

    #drop 