import pandas as pd
import os
import glob
import re

cwd = os.getcwd()
relative_data_dir = "results/qwen-2.5-72B-finetuned/"
data_dir = os.path.join(cwd, relative_data_dir)
csv_files = glob.glob(os.path.join(data_dir, "*"))
averaged_results = dict()

prefix = "finetuned-qwen_latest-"

for filename in os.listdir(data_dir):
    if filename.startswith(prefix):
        old_path = os.path.join(data_dir, filename)
        new_filename = filename[len(prefix):]

        # Remove .csv if present at the end
        if new_filename.endswith(".csv"):
            new_filename = new_filename[:-4]

        new_path = os.path.join(data_dir, new_filename)
        os.rename(old_path, new_path)
        print(f"Renamed: {filename} -> {new_filename}")
