import pandas as pd

df = pd.read_csv("query_averages.csv")

# Check for any missing (NaN) values
missing_mask = df.isnull()

# Print rows with any missing values
rows_with_missing = df[missing_mask.any(axis=1)]

if not rows_with_missing.empty:
    print("Rows with missing values:")
    print(rows_with_missing)
else:
    print("✅ All rows have valid values in every column.")