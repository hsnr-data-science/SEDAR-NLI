from sentence_transformers import SentenceTransformer, util
import numpy as np
from dotenv import load_dotenv
from openai import AzureOpenAI
import os
import json
import re

# Load environment variables
load_dotenv()

# Initialize Azure OpenAI client
client = AzureOpenAI(
    api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    api_version="2024-12-01-preview",
    azure_endpoint=os.getenv("AZURE_OPENAI_URL")
)

deployment_name = "gpt-4o-mini"

def remove_json_code_block_markers(text: str):
    match = re.search(r"```json(.*?)```", text, re.DOTALL)
    if match:
        return match.group(1).strip()
    match = re.search(r"```(.*?)```", text, re.DOTALL)
    if match:
        return match.group(1).strip()
    return text.strip()

def load_json(text: str):
    cleaned_text = remove_json_code_block_markers(text)
    return json.loads(cleaned_text)

def generate_variations(original_query, current_variations, num_variations=20):
    prompt = f"""
Generate {num_variations} semantically different variations of this query:

"{original_query}"

You can use these existing variations as a reference:
{"\n".join(current_variations)}

STRICTLY OUTPUT ONLY JSON.

Use the following JSON format:
{{
    "variations": [
        "variation 1",
        "variation 2",
        ...
    ]
}}

Try to change the sentence as much as possible. You can try questions or commands or make the sentence longer or shorter or use different words to express the intent.
"""

    response = client.chat.completions.create(
        model=deployment_name,
        messages=[
            {"role": "system", "content": "You are a helpful assistant skilled at rephrasing and generating semantically equivalent natural language queries."},
            {"role": "user", "content": prompt}
        ],
        temperature=0.7,
        max_tokens=800
    )

    result = response.choices[0].message.content
    return load_json(result)["variations"]

def compute_similarity(base_query, query_list, model_name="all-mpnet-base-v2", threshold=0.7):
    model = SentenceTransformer(model_name)
    all_queries = [base_query] + query_list
    embeddings = model.encode(all_queries, convert_to_tensor=True)
    base_embedding = embeddings[0]
    similarities = util.cos_sim(base_embedding, embeddings[1:])[0]

    print(f"\nBase Query: \"{base_query}\"\n")
    print("Similarities:")
    for i, score in enumerate(similarities):
        print(f"{base_query} || {query_list[i]} → {score:.2f}")

    filtered = [
        (base_query, query_list[i], score.item())
        for i, score in enumerate(similarities)
        if score < threshold
    ]

    print("\nPairs Below Threshold:")
    for q1, q2, score in filtered:
        print(f"{q1} || {q2} → {score:.2f}")

    return similarities, filtered

def compute_avg_similarity(base, variations, model):
    embeddings = model.encode([base] + variations, convert_to_tensor=True)
    base_emb = embeddings[0]
    variation_embs = embeddings[1:]
    similarities = util.cos_sim(base_emb, variation_embs)[0]
    return similarities.mean().item()

# Example usage
if __name__ == "__main__":
    # queries = {
    #     "Create a dataset from the file 'Capitals.json'. Then ingest it.": ["Ingest the file 'Capitals.json' into the appropriate storage system and activate versioning."],
    #     "Create an ML notebook to perform binary classification based on the tabular data in the 'Sensor_Data' dataset to predict the target variable 'failure'. Use the existing 'First Experiment'": ["Use the 'Sensor_Data' dataset to generate an ML notebook for binary classification predicting 'failure', based on the 'First Experiment'.", "Perform binary classification based on the tabular data in the 'Sensor_Data' dataset to predict the target variable 'failure'. Use the existing 'First Experiment'"],
    #     "Create a dataset called 'Chemistry' based on the uploaded file 'chemical_experiment_enzymes.csv'": ["Use the 'chemical_experiment_enzymes.csv' file to create a dataset named 'Chemistry'.", "Construct a dataset titled 'Chemistry' based on 'chemical_experiment_enzymes.csv'.", "Upload 'chemical_experiment_enzymes.csv', name the dataset 'Chemistry'."],
    #     "Upload the file 'Country.csv' as a dataset and publish it. For this add a semantic annotation/tag corresponding to the 'Country' entity from the DBPedia Ontology.": ["Upload the dataset 'Country.csv' with default settings, ingest it and publish it. Use a fitting tag from the DBPedia Ontology.", "Upload 'Country.csv' as a dataset, ingest it, and search the DBPedia ontology for a matching 'Country' tag to apply before publishing the dataset."],
    #     "Create a new ontology using the file “./data/foaf.rdf”": [],
    #     "Create an automl notebook to predict the Scores based on the other features on the 'Student Scores' dataset. Use AutoGluon. Use the existing experiment named 'First Experiment' for this.": ["Use the dataset 'Student_Scores' and create an AutoML run on it to predict the label 'Scores' based on the other features using linear regression.", "Generate a notebook using AutoGluon to perform AutoML on the 'Student Scores' dataset to predict 'Scores', using 'First Experiment' as the base."],
    #     "Create a semantic model to join the datasets Countries and Capitals. For this, predict the semantic labels for the columns based on the DBPedia ontology.": ["Build a semantic modeling to join the Countries and Capitals datasets, using column labels from the DBPedia ontology."],
    #     "Delete the dataset “Usernames_3”": ["Drop the dataset 'Usernames_3'"], #TODO
    #     "Look at the dataset “Chemical_Experiment_Organic_Compounds” and add a description to it based on your understanding": ["Search for the dataset 'Chemical_Experiment_Organic_Compounds', and add a description to it by interpreting the data."],
    #     "Search for the dataset 'Chemical_Experiment_Enzymes' then use the search to find similar datasets": ["Given the dataset 'Chemical_Experiment_Enzymes', find all datasets related to it.", "Find all datasets that are related to the 'Chemical_Experiment_Enzymes' dataset."],
    #     "Get all datasets": ["What data do I have?", "What datasets do I have?"],
    #     "Get all versions of the dataset 'Usernames_4'": ["Does 'Usernames_4' have any versions?"],
    #     "What is the description of the 'Chemical_Experiment_Reaction_Kinetics' dataset?": ["How is the dataset 'Chemical_Experiment_Reaction_Kinetics' described?", "Give me the description of the dataset 'Chemical_Experiment_Reaction_Kinetics'."],
    #     "What is the lineage of dataset 'Join Test'?": ["Can you trace the origins and connections of 'Join Test'?"],
    #     "Show me the linked datasets of the dataset 'Join Test'": ["Search for the dataset 'Join Test' and find its linked datasets."],
    #     "How many rows does the dataset “Currencies” have?": ["How many records are in the 'Currencies' dataset?", "What’s the total number of entries in the 'Currencies' dataset?"],
    #     "Show me the datalake statistics": ["How many datasets, users, workspaces and ontologies are there?"],
    #     "Create a semantic mapping based on the Countries and Capitals datasets. Label the columns using the DBPedia ontology.": ["Map the Countries and Capitals datasets semantically, using the DBPedia ontology to annotate the columns."],
    #     "What columns does the dataset “Chemical_Experiment_Materials” have?": ["What are the attributes of the “Chemical_Experiment_Materials” dataset?"],
    #     "What tags does the dataset 'Countries' have?": ["What are the annotations of the dataset 'Countries'?", "How is the dataset 'Countries' annotated?"],
    #     "List all users": ["What are the users in the current workspace?"], # (in the current workspace)
    #     "Get the semantic mapping named 'Countries_Currencies_Capitals' and join the data and query it to get all countries with their capitals and currencies.": ["Query the 'Countries_Currencies_Capitals' semantic mapping to retrieve a joined view of countries, their capitals, and their currencies."],
    #     "Search for datasets that contain data about Chemicals.": ["Identify all datasets related to Chemicals.", "Find datasets that include information about Chemicals."],
    #     "Update the title of the dataset named “Usernames_2” to “Usernames_Additonal_Infos”": ["Rename the dataset “Usernames_2” to “Usernames_Additional_Infos”", "Set the title of the dataset “Usernames_2” as “Usernames_Additional_Infos”"], # TODO
    #     "Upload the two datasets 'chemical_experiment_organic_properties.csv' and 'chemical_experiment_reaction_kinetics.csv' and run their ingestion concurrently": ["Upload the datasets 'chemical_experiment_organic_properties.csv' and 'chemical_experiment_reaction_kinetics.csv' and ingest them"],
    #     "How many versions does dataset 'Usernames_4' have? What are their differences?": ["Show me the number of versions for 'Usernames_4' and highlight their key differences.", "How many updates has 'Usernames_4' had, and what changes were made?"],
    #     "What is the title of the current workspace?": ["What’s the name of the workspace?"]
    # }

    # threshold = 0.85
    # required_variations = 5
    # final_variations = {}

    # index = 1

    # for base_query, existing in queries.items():
    #     print(f"\n--- Processing: {base_query} ---")

    #     # Step 1: Keep existing ones below the threshold
    #     similarities, filtered_existing = compute_similarity(base_query, existing, threshold=threshold)
    #     current_variations = [v for _, v, _ in filtered_existing]

    #     # Step 2: Generate more if needed
    #     attempts = 0
    #     max_attempts = 10
    #     while len(current_variations) < required_variations and attempts < max_attempts:
    #         generated = generate_variations(base_query, current_variations + existing, num_variations=20)
    #         similarities, filtered_generated = compute_similarity(base_query, generated, threshold=threshold)

    #         for _, variation, _ in filtered_generated:
    #             if variation not in current_variations:
    #                 current_variations.append(variation)
    #             if len(current_variations) == required_variations:
    #                 break
    #         attempts += 1

    #     final_variations[base_query] = current_variations

    #     # Print final variations
    #     print(f"\n✅ Final Variations ({len(current_variations)}):")
    #     for v in current_variations:
    #         print(f"- {v}")

    #     # Save this query's variations to a separate file immediately
    #     filename = f"{index}.json"
    #     data_to_save = {
    #         "base_query": base_query,
    #         "variations": current_variations
    #     }
    #     with open(filename, "w", encoding="utf-8") as f:
    #         json.dump(data_to_save, f, indent=4, ensure_ascii=False)
    #     print(f"✅ Saved to {filename}")

    #     index += 1  # Increment file number

    model = SentenceTransformer("all-mpnet-base-v2")
    json_files = sorted([f for f in os.listdir() if f.endswith(".json")])

    total_avg = 0
    count = 0

    print("🔍 Average Similarity Per Base Query:\n")

    for filename in json_files:
        with open(filename, "r", encoding="utf-8") as f:
            data = json.load(f)
            base_query = data["base_query"]
            variations = data["variations"]
            if variations:
                avg_sim = compute_avg_similarity(base_query, variations, model)
                print(f"{base_query}: {avg_sim:.4f}")
                total_avg += avg_sim
                count += 1

    overall_avg = total_avg / count
    print(f"\n🌍 Global Average Similarity Across All Queries: {overall_avg:.4f}")








# What does this paper say about my question? Or what does it suggest in regards to my question?

# QUESTION:
# I have two queries to evaluate my AI system, e.g.:
# Get all datasets,
# Please provide me with all available datasets

# And I measure the semantic similarity between them (At the moment using cosine similarity and all-MiniLM-L6-v2). I have multiple of such queries with the same meaning.

# Which threshold should be used to have the most diverse queries but still the same meaning? Or how can I determine when a query is dissimilar enough but still has the same meaning?


# PAPER:



# The paper "A Study of Sentence Similarity Based on the All-minilml6-v2 Model With 'Same Semantics, Different Structure' After Fine Tuning" by Chen Yin and Zixuan Zhang provides insights into evaluating sentence similarity using the all-MiniLM-L6-v2 model. Here are the key points relevant to your question:

# Model Performance and Fine-Tuning: The paper demonstrates that fine-tuning the all-MiniLM-L6-v2 model using meta-learning and few-shot learning methods significantly improves its performance in distinguishing sentences with "similar structure, different semantics" and "different structure, similar semantics". The fine-tuned model achieved a precision increase from 0.74 to 0.91, indicating its enhanced ability to accurately judge sentence similarity.
# Evaluation Metrics: The paper suggests using multiple evaluation metrics beyond traditional accuracy, such as precision, recall, and ROC AUC, to better assess the model's performance. This approach helps in identifying the most effective threshold for semantic similarity.
# Threshold Determination: The paper does not provide a specific threshold value but implies that fine-tuning and evaluating using diverse metrics can help determine the optimal threshold for semantic similarity. The introduction of ROC curves and precision-recall metrics allows for a more nuanced understanding of the model's performance, which can guide the selection of a threshold that balances precision and recall.
# Model Comparison: The paper compares the all-MiniLM-L6-v2 model with BERT and BERT+bidirectional LSTM models, showing that the all-MiniLM-L6-v2 model, especially after fine-tuning, performs better in the task of sentence similarity.

# To determine when a query is dissimilar enough but still has the same meaning, you should:
# Fine-Tune Your Model: Consider fine-tuning your all-MiniLM-L6-v2 model using meta-learning and few-shot learning techniques as described in the paper.
# Use Multiple Evaluation Metrics: Evaluate your model using precision, recall, and ROC AUC to get a comprehensive view of its performance.
# Experiment with Thresholds: Experiment with different threshold values and use the evaluation metrics to find a balance that ensures diverse queries are recognized as having the same meaning.

# By following these steps, you can improve the semantic similarity measurement of your AI system and determine the optimal threshold for diverse queries with the same meaning.
    

# The paper "Exploration of a Threshold for Similarity based on Uncertainty in Word Embedding" by Navid Rekabsaz, Mihai Lupu, and Allan Hanbury addresses the issue of determining a threshold for semantic similarity in word embeddings. This threshold is crucial for identifying related terms while filtering out unrelated ones. Here are the key points and suggestions from the paper relevant to your question:

# Uncertainty in Similarity Values: The paper quantifies the uncertainty in word embedding models with respect to the similarity values they generate. This uncertainty arises because word embeddings are typically initialized randomly and converge to slightly different solutions even when trained with the same parameters and data. The paper observes that the uncertainty decreases as the similarity value increases and also decreases with higher dimensionality of the embeddings.
# Continuous Distribution of Neighbours: The authors introduce a novel representation of the expected number of neighbours around an arbitrary term as a continuous function over similarity values. This representation accounts for the uncertainty in similarity values and provides a smooth, monotonic estimation of the number of similar terms.
# Threshold Determination: The paper proposes a general threshold for separating semantically related terms from unrelated ones. This threshold is estimated based on the average number of synonyms for a word in English, derived from WordNet. The authors find that the average number of synonyms is 1.6, and they use this value to define the threshold for different embedding dimensions.
# Evaluation: The proposed threshold is evaluated on four test collections using several relevance scoring models (BM25 and Language Model combined with Generalized and Extended Translation models). The results show that the proposed threshold performs either exactly the same as, or statistically indistinguishable from, the optimal threshold obtained by exhaustive search.
# Recommendations for Your Question:
 

# Threshold Value: The paper suggests using a threshold that corresponds to the average number of synonyms in WordNet (1.6). For different embedding dimensions, the thresholds are:
# 100 dimensions: 0.818
# 200 dimensions: 0.756
# 300 dimensions: 0.708
# 400 dimensions: 0.675
# Determining Dissimilarity: The threshold should be set to filter terms that have a similarity value lower than the proposed threshold. This ensures that only terms with high semantic relatedness are considered similar.
# Evaluation Method: To determine when a query is dissimilar enough but still has the same meaning, you can use the proposed threshold values. If the cosine similarity between two queries is above the threshold, they can be considered similar. If it is below the threshold, they are dissimilar.
# Practical Application:
# For your AI system that evaluates semantic similarity between queries, you can use the proposed thresholds to filter out unrelated queries. For example, if the cosine similarity between "Get all datasets" and "Please provide me with all available datasets" is above the threshold (e.g., 0.756 for 200-dimensional embeddings), they can be considered to have the same meaning. If the similarity is below this threshold, they are dissimilar.

# By applying these thresholds, you can ensure that your system identifies diverse queries that still convey the same meaning, improving the accuracy and relevance of your AI evaluations.


# Yes, the paper "Exploration of a Threshold for Similarity based on Uncertainty in Word Embedding" by Navid Rekabsaz, Mihai Lupu, and Allan Hanbury addresses whether cosine similarity with embeddings and some threshold can be used to determine the semantic similarity of texts or queries.

# The paper investigates the uncertainty of word embedding models with respect to the similarity values they generate and introduces a general threshold to effectively filter related terms. The authors explore the effect of dimensionality on this threshold by conducting experiments with different vector dimensions. Their evaluation on multiple test collections and relevance scoring models supports the effectiveness of their approach, showing that the proposed threshold performs significantly better than the baseline and is statistically indistinguishable from the optimal results.

# In summary, the paper provides a method to identify a threshold for cosine similarity values in word embeddings that can be used to determine semantic similarity between terms, demonstrating its practical use in information retrieval tasks.


# Yes, the paper "On the Various Semantics of Similarity in Word Embedding Models" addresses whether cosine similarity with embeddings and some threshold can be used to determine how semantically similar some texts or queries are. The authors examine the statistical distribution of similarity values in word embedding models and explore the notion of similarity in these models. They conclude that intuitive similarity thresholds do not exist universally across different models and parameter settings. Instead, they propose a method to determine meaningful similarity values for a specific embedding model, based on statistical tests and comparisons with a baseline such as WordNet similarity measures. This approach allows for identifying ranges of similarity values where comparisons are meaningful, thus providing a more reliable way to assess semantic similarity using cosine similarity and embeddings.

