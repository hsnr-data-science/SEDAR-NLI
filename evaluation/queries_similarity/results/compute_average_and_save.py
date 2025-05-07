import pandas as pd
import os
import glob
import re

cwd = os.getcwd()
relative_data_dir = "results/qwen-2.5-72B-finetuned/"
data_dir = os.path.join(cwd, relative_data_dir)
csv_files = glob.glob(os.path.join(data_dir, "*"))
averaged_results = dict()

prefix = "qwen-2.5-72B-finetuned-"

filename_pattern = re.compile(r"(?P<llm>[^-]+-[^/]+)-(?P<query>[^-]+(?:-[^-/]+)*)-.*")


columns_to_average = [
    "latency", "tokens", "tools_exact_match", "nodes_jaccard_similarity",
    "tools_extra_steps", "pass@1", "nodes_exact_match", "nodes_longest_common_subsequence",
    "tools_longest_common_subsequence", "human_correct", "nodes_edit_distance", "pass@2",
    "tools_jaccard_similarity", "tools_edit_distance", "nodes_unmatched_steps",
    "tools_unmatched_steps", "nodes_extra_steps", "is_correct"
]

rounding_map = {
    "latency": 2,
    "tokens": 0,
    "tools_exact_match": 2,
    "nodes_jaccard_similarity": 2,
    "tools_extra_steps": 2,
    "pass@1": 2,
    "nodes_exact_match": 2,
    "nodes_longest_common_subsequence": 2,
    "tools_longest_common_subsequence": 2,
    "human_correct": 2,
    "nodes_edit_distance": 2,
    "pass@2": 2,
    "tools_jaccard_similarity": 2,
    "tools_edit_distance": 2,
    "nodes_unmatched_steps": 2,
    "tools_unmatched_steps": 2,
    "nodes_extra_steps": 2,
    "is_correct": 2
}

averaged_results = {}

for file_path in csv_files:
    file_name = os.path.basename(file_path)
    match = re.match(r"(?P<query>.+)-[a-f0-9]{8}$", file_name)

    if match:
        query_name = match.group("query")
        df = pd.read_csv(file_path)

        if df["is_correct"].isna().any():
            print(f"{query_name} → is_correct has NaN")

        # Convert categorical to numeric
        df["human_correct"] = df["human_correct"].map({"YES": 1, "NO": 0})

        # Filter and average
        filtered_df = df[columns_to_average].copy()
        averaged_results[query_name] = filtered_df.mean(numeric_only=True)

# Create DataFrame from query averages
summary_df = pd.DataFrame.from_dict(averaged_results, orient="index")

# Add a final row for the overall average
summary_df.loc["OVERALL_AVERAGE"] = summary_df.mean(numeric_only=True)

# Apply rounding to all columns
for column in summary_df.columns:
    if column in rounding_map:
        summary_df[column] = summary_df[column].round(rounding_map[column])

# Save query averages (excluding "OVERALL_AVERAGE")
query_averages = summary_df.drop("OVERALL_AVERAGE")
query_averages.to_csv("query_averages.csv")

# Get the overall average row
overall_average = summary_df.loc["OVERALL_AVERAGE"].to_frame().T

# Append to overall_average.csv, handling line breaks correctly
llm_name = prefix[:-1]
overall_average.insert(0, "llm", llm_name)
file_path = "overall_average.csv"
if os.path.exists(file_path):
    # If the file exists, append the row without writing the header
    overall_average.to_csv(file_path, mode='a', header=False, index=False, lineterminator='\n')
else:
    # If the file does not exist, write the header and the first row
    overall_average.to_csv(file_path, mode='w', header=True, index=False, lineterminator='\n')

print("Files saved: query_averages.csv and appended to overall_average.csv")