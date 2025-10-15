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

    #drop columns
    for col in ["slug", "original_method"]:
        if col in df.columns:
            df = df.drop(columns=[col])
            print(f"– Removed column: {col}")

    #tidy up text columns 
    for col in df.select_dtypes(include="object").columns:
        df[col] = (
            df[col]
            .astype(str)
            .str.strip()
            .replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
        )
    #parse dates if exist
    for date_col in ["event_date", "reported_date"]:
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    
    #hand to have year/moth
    if "event_date" in df.columns:
        if "year" not in df.columns:
            df["year"] = df["event_date"].dt.year
        if "month" not in df.columns:
            df["month"] = df["event_date"].dt.month

    # keep events roughly in project window (2014–2025)
    if "year" in df.columns:
        before = len(df)
        df = df[df["year"].between(2014, 2025, inclusive="both") | df["year"].isna()]
        after = len(df)
        if after != before:
            print(f"– Filtered by year 2014–2025: {before} → {after}")

    #drop if is 95% + empty
    null_ratio = df.isna().mean()
    sparse_cols = [c for c, r in null_ratio.items() if r > 0.95]
    if sparse_cols:
        df = df.drop(columns=sparse_cols)
        print("– Dropped very sparse columns:", ", ".join(sparse_cols))

    #de duplicate just in case
    before = len(df)
    df = df.drop_duplicates()
    print(f"– Duplicates removed: {before - len(df)}")

    # save file
    df.to_csv(OUT_FILE, index=False)
    print(f"Saved cleaned dataset to: {OUT_FILE}")
    print("Final shape:", df.shape)


if __name__ == "__main__":
    main()