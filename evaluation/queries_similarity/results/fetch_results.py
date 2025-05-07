import os
import re
import requests
import pandas as pd
from io import StringIO
from collections import defaultdict
from langsmith import Client
from dotenv import load_dotenv

def extract_query_name(project_name, llm_name):
    """
    Extract the query name from a project name.
    
    Example: 
    'claude-3-7-sonnet-20250219-Delete-Dataset-effda91e' -> 'Delete-Dataset'
    
    Args:
        project_name: Full project name
        llm_name: LLM name to remove from the beginning
        
    Returns:
        Query name or None if pattern doesn't match
    """
    # Remove the LLM name prefix
    if project_name.startswith(llm_name):# and not "mini" in project_name:
        # Remove the LLM name and the dash that follows it
        remaining = project_name[len(llm_name):]
        if remaining.startswith('-'):
            remaining = remaining[1:]
            
        # Remove the random ID at the end (last segment after dash)
        parts = remaining.rsplit('-', 1)
        if len(parts) > 1:
            return parts[0]
    
    return None

def process_llm_data(llm_name):
    """
    Process LangSmith data for a given LLM, filtering out errored runs
    and combining duplicate query projects.
    
    Args:
        llm_name: Name of the LLM (e.g., 'claude-3-7-sonnet-20250219')
    
    Returns:
        Dictionary mapping query names to their combined dataframes
    """
    print(f"Processing data for LLM: {llm_name}")
    
    # Load environment variables
    load_dotenv()
    api_key = os.getenv("LANGCHAIN_API_KEY")
    
    if not api_key:
        raise ValueError("LANGCHAIN_API_KEY not found in environment variables")
    
    # Initialize LangSmith client
    client = Client()
    
    # Get all projects for this LLM
    projects = []
    query_projects = defaultdict(list)
    
    print("Fetching projects for LLM:", llm_name)
    for project in client.list_projects():
        if llm_name in project.name:
            projects.append(project)
            
            # Extract query name and organize by query
            query_name = extract_query_name(project.name, llm_name)
            if query_name:
                query_projects[query_name].append(project.id)
                print(f"Found project: {project.name} -> Query: '{query_name}'")
    
    print(f"\nFound {len(query_projects)} unique queries")
    for query, project_ids in query_projects.items():
        print(f"  {query}: {len(project_ids)} project(s)")
    
    # Process each query and combine duplicate projects
    results = {}
    
    for query_name, project_ids in query_projects.items():
        print(f"\nProcessing query: {query_name}")
        combined_df = pd.DataFrame()
        
        for project_id in project_ids:
            
            # Make request to get CSV data
            url = "https://api.smith.langchain.com/datasets/7dbccb8a-f70a-4bec-8374-e6d6ea30e5cf/runs"
            headers = {"x-api-key": api_key}
            params = {"format": "csv"}
            payload = {"session_ids": [str(project_id)]}
            
            try:
                response = requests.post(url, headers=headers, json=payload, params=params)
                response.raise_for_status()
                
                # Process CSV data
                csv_content = response.text
                df = process_csv_with_errors(csv_content)
                
                # Filter only successful runs
                df_success = df[df['status'] == 'success']
                
                if len(df_success) > 0:
                    print(f"    Success! Got {len(df_success)} successful runs")
                    # Combine with previous results for this query
                    if combined_df.empty:
                        combined_df = df_success
                    else:
                        combined_df = pd.concat([combined_df, df_success], ignore_index=True)
                else:
                    print(f"    No successful runs found")
                    
            except requests.exceptions.RequestException as e:
                print(f"    Error making request: {e}")
            except Exception as e:
                print(f"    Error processing data: {e}")
        
        if not combined_df.empty:
            results[query_name] = combined_df
            print(f"  Combined {len(combined_df)} successful runs for query: {query_name}")
    
    return results

def process_csv_with_errors(csv_content):
    """
    Process CSV content that might contain error traces.
    
    This function handles CSV files where some rows might contain Python
    tracebacks spanning multiple lines.
    """
    try:
        # First attempt to read as normal CSV
        return pd.read_csv(StringIO(csv_content))
    except pd.errors.ParserError:
        # Handle CSV with errors by reading line by line
        lines = csv_content.splitlines()
        
        # Get header
        header = lines[0]
        
        # Process content row by row
        clean_rows = [header]
        i = 1
        while i < len(lines):
            row = lines[i]
            
            # Skip empty lines
            if not row.strip():
                i += 1
                continue
                
            # Check if this is the start of a valid CSV row (should start with an ID)
            if row.startswith('"') or row[0].isalnum():
                # It might be a multi-line row with error traces
                complete_row = row
                
                # Keep adding lines until we reach the end of the error trace
                # or another row that looks like a valid CSV row
                j = i + 1
                while j < len(lines):
                    next_line = lines[j]
                    if (next_line.startswith('"') or 
                        (next_line and next_line[0].isalnum() and ',' in next_line[:100])):
                        # This appears to be the start of a new row
                        break
                    complete_row += "\n" + next_line
                    j += 1
                
                clean_rows.append(complete_row)
                i = j
            else:
                # Skip this line as it's likely part of an error trace
                i += 1
        
        # Create DataFrame from cleaned rows
        return pd.read_csv(StringIO('\n'.join(clean_rows)))

def ensure_five_rows(results):
    """
    Ensure each query dataframe has exactly 5 rows.
    If fewer, add error entries with worst scores.
    
    Args:
        results: Dictionary of query name to dataframe
    
    Returns:
        Dictionary with all dataframes having 5 rows
    """
    # Define error row template (worst possible scores)
    error_row_data = {
        'status': 'success',
        'error': '',
        'latency': 45.0909309387207,
        'tokens': 30000,
        'total_cost': 0.01294335,
        'tools_extra_steps': 0.0,
        'is_correct': 0.0,
        'tools_unmatched_steps': 0.0,
        'tools_edit_distance': 1.0,
        'nodes_jaccard_similarity': 0.0,
        'evaluate_edit_distance': 1.0,
        'nodes_extra_steps': 0.0,
        'tools_exact_match': 0.0,
        'nodes_edit_distance': 1.0,
        'tools_jaccard_similarity': 0.0,
        'human_correct': 'NO',
        'nodes_exact_match': 0.0,
        'evaluate_unmatched_steps': 1.0,
        'evaluate_extra_steps': 1.0,
        'evaluate_longest_common_subsequence': 0.0,
        'pass@2': 0.0,
        'evaluate_exact_match': 0.0,
        'evaluate_jaccard_similarity': 0.0,
        'nodes_longest_common_subsequence': 0.0,
        'nodes_unmatched_steps': 0.0,
        'tools_longest_common_subsequence': 0.0,
        'pass@1': 0.0
    }

    fixed_results = {}
    expected_rows = 5

    for query_name, df in results.items():
        num_rows = len(df)
        if num_rows < expected_rows:
            print(f"Query '{query_name}' has {num_rows} rows. Adding {expected_rows - num_rows} error rows.")

            # Use the first row as a template, or create a blank one if df is empty
            if num_rows > 0:
                template_row = df.iloc[0].copy()
            else:
                # If the dataframe is empty, create a row with all columns set to None
                template_row = pd.Series({col: None for col in df.columns})

            error_rows = []
            for _ in range(expected_rows - num_rows):
                error_row = template_row.copy()
                # Overwrite columns that exist in both template and error_row_data
                for col in df.columns:
                    if col in error_row_data:
                        error_row[col] = error_row_data[col]
                error_rows.append(error_row)

            # Append error rows to the dataframe
            error_df = pd.DataFrame(error_rows, columns=df.columns)
            fixed_df = pd.concat([df, error_df], ignore_index=True)
            fixed_results[query_name] = fixed_df
        else:
            fixed_results[query_name] = df
  
    return fixed_results

def validate_results(results):
    """
    Validate the results to ensure we have the expected data:
    - 27 total dataframes (queries)
    - 5 rows per query
    """
    num_queries = len(results)
    expected_queries = 27
    expected_rows = 5
    
    print("\nValidation Results:")
    print(f"Total queries: {num_queries} (Expected: {expected_queries})")
    
    if num_queries != expected_queries:
        print(f"WARNING: Expected {expected_queries} queries but found {num_queries}")
    
    for query_name, df in results.items():
        num_rows = len(df)
        if num_rows != expected_rows:
            print(f"WARNING: Query '{query_name}' has {num_rows} rows (Expected: {expected_rows})")
    
    # Count queries with exactly 5 rows
    queries_with_5_rows = sum(1 for df in results.values() if len(df) == expected_rows)
    print(f"Queries with exactly {expected_rows} rows: {queries_with_5_rows}/{num_queries}")
    
    return num_queries == expected_queries and all(len(df) == expected_rows for df in results.values())

def calculate_averages(results, llm_name):
    """
    Calculate averages for each query and overall averages.
    
    Args:
        results: Dictionary of query name to dataframe
        llm_name: Name of the LLM
        
    Returns:
        Tuple of (query_averages, overall_average)
    """
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
    
    for query_name, df in results.items():
        print(f"Processing averages for {query_name}")
        
        if "human_correct" in df.columns:
            # Convert categorical to numeric
            df["human_correct"] = df["human_correct"].map({"YES": 1, "NO": 0})
        
        # Check for NaN values in is_correct column
        if "is_correct" in df.columns and df["is_correct"].isna().any():
            print(f"{query_name} → is_correct has NaN values")
        
        # Filter columns and calculate mean
        available_columns = [col for col in columns_to_average if col in df.columns]
        if available_columns:
            filtered_df = df[available_columns].copy()
            averaged_results[query_name] = filtered_df.mean(numeric_only=True)
    
    # Create DataFrame from query averages
    summary_df = pd.DataFrame.from_dict(averaged_results, orient="index")
    
    # Add a final row for the overall average
    summary_df.loc["OVERALL_AVERAGE"] = summary_df.mean(numeric_only=True)
    
    # Apply rounding to all columns
    for column in summary_df.columns:
        if column in rounding_map:
            summary_df[column] = summary_df[column].round(rounding_map[column])
    
    # Get the overall average row
    overall_average = summary_df.loc["OVERALL_AVERAGE"].to_frame().T
    overall_average.insert(0, "llm", llm_name)
    
    # Get query averages (excluding "OVERALL_AVERAGE")
    query_averages = summary_df.drop("OVERALL_AVERAGE")
    
    return query_averages, overall_average

def main():
    llm_name = "gpt-4.1-PC"
    results = process_llm_data(llm_name)

    results = ensure_five_rows(results)
    
    # Validate results
    is_valid = validate_results(results)
    
    # Save results to CSV files
    print("\nSaving individual query results...")
    results_dir = f"results/{llm_name}"
    os.makedirs(results_dir, exist_ok=True)
    
    for query_name, df in results.items():
        filename = f"{results_dir}/{llm_name}_{query_name}_combined.csv"
        df.to_csv(filename, index=False)
        print(f"Saved {len(df)} rows to {filename}")
    
    # Calculate and save averages
    print("\nCalculating averages...")
    query_averages, overall_average = calculate_averages(results, llm_name)
    
    # Save query averages
    query_averages_file = f"{llm_name}_query_averages.csv"
    query_averages.to_csv(query_averages_file)
    print(f"Saved query averages to {query_averages_file}")
    
    # Append to overall_average.csv, handling line breaks correctly
    overall_average_file = "overall_average.csv"
    if os.path.exists(overall_average_file):
        # If the file exists, append the row without writing the header
        overall_average.to_csv(overall_average_file, mode='a', header=False, index=False, lineterminator='\n')
        print(f"Appended overall average for {llm_name} to {overall_average_file}")
    else:
        # If the file does not exist, write the header and the first row
        overall_average.to_csv(overall_average_file, mode='w', header=True, index=False, lineterminator='\n')
        print(f"Created {overall_average_file} with overall average for {llm_name}")
    
    # Provide summary
    print(f"\nProcessed {len(results)} unique queries")
    if is_valid:
        print("✅ All validation checks passed!")
    else:
        print("⚠️ Some validation checks failed. Please review the warnings above.")

if __name__ == "__main__":
    main()