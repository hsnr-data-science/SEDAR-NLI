import pandas as pd

# Read the CSV file
df = pd.read_csv('overall_average.csv')

# Create abbreviations for long column names
abbreviations = {
    'tokens': 'TOK',
    'tools_exact_match': 'TEM',
    'nodes_jaccard_similarity': 'NJS',
    'tools_extra_steps': 'TES',
    'pass@1': 'P@1',
    'nodes_exact_match': 'NEM',
    'nodes_longest_common_subsequence': 'NLCS',
    'tools_longest_common_subsequence': 'TLCS',
    'human_correct': 'ACC',
    'nodes_edit_distance': 'NED',
    'pass@2': 'P@2',
    'tools_jaccard_similarity': 'TJS',
    'tools_edit_distance': 'TED',
    'nodes_unmatched_steps': 'NUS',
    'tools_unmatched_steps': 'TUS',
    'nodes_extra_steps': 'NES',
    'is_correct': 'LACC'
}

# Define whether higher (1) or lower (0) values are better for each metric
higher_is_better = {
    'tokens': 0,
    'tools_exact_match': 1,
    'nodes_jaccard_similarity': 1,
    'tools_extra_steps': 1,
    'pass@1': 1,
    'nodes_exact_match': 1,
    'nodes_longest_common_subsequence': 1,
    'tools_longest_common_subsequence': 1,
    'human_correct': 1,
    'nodes_edit_distance': 0,
    'pass@2': 1,
    'tools_jaccard_similarity': 1,
    'tools_edit_distance': 0,
    'nodes_unmatched_steps': 1,
    'tools_unmatched_steps': 1,
    'nodes_extra_steps': 1,
    'is_correct': 1
}

# Add detailed descriptions for metrics - with proper LaTeX escaping
metric_descriptions = {
    'extra_steps': 'Normalized inverse of extra steps (1 - extra\\_steps/MAX\\_LENGTH). Higher is better, indicating fewer unnecessary steps.',
    'unmatched_steps': 'Normalized inverse of unmatched steps (1 - unmatched\\_steps/MAX\\_LENGTH). Higher is better, indicating better alignment with expected sequence.',
    'jaccard_similarity': 'Ratio of intersection to union of steps, measuring overlap between sequences.',
    'exact_match': 'Binary score for whether sequences match exactly.',
    'longest_common_subsequence': 'Length of the longest common subsequence relative to the reference sequence.',
    'edit_distance': 'TODOOOOOONormalized inverse of edit distance (1 - edit\\_distance/MAX\\_LENGTH). Higher means less edits needed to match sequences.'
}

# Define tables with their columns
tables = {
    "short_overview": {
        "title": "Selection of LLMs and the most important metrics",
        "columns": ["llm", "human_correct", "is_correct", "pass@1", "nodes_exact_match", "tools_exact_match"]
    },
    "model_and_performance": {
        "title": "Model Performance",
        "columns": ["llm", "tokens", "is_correct", "human_correct", "pass@1", "pass@2"]
    },
    "node_metrics": {
        "title": "Node Metrics",
        "columns": ["llm", "nodes_exact_match", "nodes_jaccard_similarity", 
                   "nodes_longest_common_subsequence", "nodes_edit_distance", 
                   "nodes_unmatched_steps", "nodes_extra_steps"]
    }, 
    "tool_metrics": {
        "title": "Tool Metrics",
        "columns": ["llm", "tools_exact_match", "tools_jaccard_similarity", 
                   "tools_longest_common_subsequence", "tools_edit_distance", 
                   "tools_unmatched_steps", "tools_extra_steps"]
    }
}

# Function to find best values for each column across the dataset
def find_best_values(df, higher_is_better):
    best_values = {}
    for column in df.columns:
        if column in higher_is_better:
            if higher_is_better[column] == 1:  # Higher is better
                best_values[column] = df[column].max()
            else:  # Lower is better
                best_values[column] = df[column].min()
    return best_values

# Get best values for each column
best_values = find_best_values(df, higher_is_better)

# Function to format value based on column type and whether it's the best value
def format_value(val, col, best_values):
    formatted_val = ""
    if col == "llm":
        return val
    elif col == "tokens":
        formatted_val = f"{int(val):,}"
    elif isinstance(val, float):
        formatted_val = f"{val:.2f}"  # Just keep 2 decimal places for floats
    else:
        formatted_val = str(val)
    
    # Highlight if this is the best value
    if col in best_values and val == best_values[col]:
        return f"\\textbf{{{formatted_val}}}"
    else:
        return formatted_val

def create_latex_table(table_name, table_info, best_values):
    table_title = table_info["title"]
    columns = table_info["columns"]
    
    headers = []
    for i, col in enumerate(columns):
        if i == 0:
            headers.append("\\textbf{Model}")
        else:
            abbr = abbreviations.get(col, col)
            # Add arrow indicator based on whether higher or lower is better
            if col in higher_is_better:
                arrow = "\\textuparrow" if higher_is_better[col] == 1 else "\\textdownarrow"
                headers.append(f"\\textbf{{{abbr}}} {arrow}")
            else:
                headers.append(f"\\textbf{{{abbr}}}")
    
    header_row = " & ".join(headers) + " \\\\"
    
    # Create data rows with best values highlighted
    data_rows = []
    for _, row in df.iterrows():
        formatted_values = [format_value(row[col], col, best_values) for col in columns]
        data_rows.append(" & ".join(formatted_values) + " \\\\")
    
    column_format = "|l|" + "c|"*(len(columns)-1)
    
    table = f"""
\\begin{{table}}[H]
\\centering
\\begin{{tabular}}{{{column_format}}}
\\hline
{header_row}
\\hline
{" ".join(data_rows)}
\\hline
\\end{{tabular}}
\\caption{{{table_title}}}
\\end{{table}}
"""
    return table

# Function to escape special characters in LaTeX
def escape_latex(text):
    # Replace special characters with LaTeX-safe versions
    replacements = {
        "_": "\\_",
        "@": "\\texttt{@}",
        "#": "\\#",
        "$": "\\$",
        "%": "\\%",
        "&": "\\&",
        "~": "\\textasciitilde{}",
        "^": "\\textasciicircum{}"
    }
    
    for char, replacement in replacements.items():
        text = text.replace(char, replacement)
    
    return text

# Create the legend with escaped special characters
legend_rows = []
for full, abbr in sorted(abbreviations.items()):
    # Make the name more readable and escape special characters
    readable = ' '.join(word.capitalize() for word in escape_latex(full).split('\\_'))
    
    # Find if there's a detailed description for this metric
    description = ""
    for keyword, desc in metric_descriptions.items():
        if keyword in full:
            description = f" ({desc})"
            break
    
    # Add information about whether higher or lower is better
    if full in higher_is_better:
        better_info = "Higher is better" if higher_is_better[full] == 1 else "Lower is better"
        if description:
            description = f"{description} {better_info}."
        else:
            description = f" ({better_info})"
    
    legend_rows.append(f"{abbr} & {readable}{description} \\\\")

# Split the legend into two separate tables for better fit
legend_half = len(legend_rows) // 2
legend_table1 = f"""
\\begin{{table}}[H]
\\centering
\\small
\\begin{{tabular}}{{|p{{0.15\\textwidth}}|p{{0.75\\textwidth}}|}}
\\hline
\\textbf{{Abbreviation}} & \\textbf{{Description}} \\\\
\\hline
{" ".join(legend_rows[:legend_half])}
\\hline
\\end{{tabular}}
\\caption{{Metrics Legend (Part 1)}}
\\end{{table}}
"""

legend_table2 = f"""
\\begin{{table}}[H]
\\centering
\\small
\\begin{{tabular}}{{|p{{0.15\\textwidth}}|p{{0.75\\textwidth}}|}}
\\hline
\\textbf{{Abbreviation}} & \\textbf{{Description}} \\\\
\\hline
{" ".join(legend_rows[legend_half:])}
\\hline
\\end{{tabular}}
\\caption{{Metrics Legend (Part 2)}}
\\end{{table}}
"""

# Generate all tables with best values highlighted
all_tables = []
for table_name, table_info in tables.items():
    all_tables.append(create_latex_table(table_name, table_info, best_values))

# Add a methodology section explaining the metrics with properly escaped math and highlighting
methodology = """
\\section*{Methodology Notes}
The metrics used in these tables evaluate model performance in different ways:

\\begin{itemize}
    \\item \\textbf{Extra Steps}: Calculated as $1 - \\frac{\\text{extra\\_steps}}{\\text{MAX\\_SEQUENCE\\_LENGTH}}$, where extra steps are the additional steps beyond what was needed. Higher scores indicate fewer unnecessary steps.
    
    \\item \\textbf{Unmatched Steps}: Calculated as $1 - \\frac{\\text{unmatched\\_steps}}{\\text{MAX\\_SEQUENCE\\_LENGTH}}$, where unmatched steps are those that don't align with the reference sequence. Higher scores indicate better sequence matching.
    
    \\item All metrics are normalized to the range [0,1], with higher values indicating better performance (except where noted otherwise in the legend).
    
    \\item \\textbf{Bold values} indicate the best performance for each metric across all models.
\\end{itemize}
"""

# Final LaTeX document
latex_document = f"""
\\documentclass{{article}}
\\usepackage{{geometry}}
\\usepackage{{caption}}
\\usepackage{{array}}
\\usepackage{{float}}      % For H placement specifier
\\usepackage{{textcomp}}   % For special text characters
\\usepackage{{amsmath}}    % For math formatting
\\geometry{{margin=1in}}

\\begin{{document}}

{" ".join(all_tables)}

{legend_table1}

{legend_table2}

{methodology}

\\end{{document}}
"""

# Save to file
with open('llm_performance_tables.tex', 'w') as f:
    f.write(latex_document)

print("LaTeX tables saved to llm_performance_tables.tex")