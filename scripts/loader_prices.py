import pandas as pd


def load_prices():
    return pd.read_csv("data/raw/prices/price_data.csv")

# Add this block to run it directly and show output
if __name__ == "__main__":
    df = load_prices()
    print(df.head())   # Show first 5 rows
