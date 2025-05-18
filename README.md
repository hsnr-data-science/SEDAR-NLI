# SEDAR-NLI
Natural Language Interface for SEDAR based on a multi-agent LLM system.
  
This repository contains the code for our CIKM 2025 paper on a multi-agent natural language interface (NLI) for semantic data lakes. The system enables users to interact with the SEDAR data lake platform using plain language, making advanced data management, discovery, and analytics accessible to non-technical users.  
  
Our approach integrates large language models (LLMs) in a modular, multi-agent architecture. By combining retrieval-augmented generation (RAG) and dynamic tool-calling, the system translates user queries into structured API calls to execute complex workflows over the data lake.  
  
Main features:  
- Multi-agent orchestration for complex query decomposition and execution  
- Retrieval-augmented generation for relevant API/tool selection  
- Automatic tool-calling for seamless backend integration  
- Evaluation framework for correctness and robustness
- We performed finetuning on a dedicated dataset specifically tailored for this system.
  

The repository includes code, datasets, and evaluation scripts.

## Repository Structure

### Main Code

- [main.py](main.py): Entry point for the system without chainlit chat.
- [chainlit_chat](chainlit_chat.py): Entry point for the system with the [Chainlit](https://github.com/NielsMittelstaedt/chainlit) interface.
- [agent_graph/](agent_graph/): Multi-agent orchestration logic.
- [agents/](agents/): Agent implementations.
- [models/](models/): Model configuration and management.
- [sedarapi/](sedarapi/): API integration with the SEDAR data lake.
- [prompts/](prompts/): Prompt templates and compression logic.
- [tools/](tools/): Tool definitions for agent actions.
- [utils/](utils/): Utility functions.

### Evaluation


- [evaluation/](evaluation/): Contains all evaluation code.
  - [evaluation/evaluation.py](evaluation/evaluation.py): Script for running quantitative evaluation.
  - [evaluation/queries_similarity/](evaluation/queries_similarity/): Semantic variations of queries.
    - Purpose: Tests how well the system handles phrasing differences and interprets reworded queries consistently.
    - [llms/](evaluation/queries_similarity/llms/): Contains results for different LLMs.
    - [queries/](evaluation/queries_similarity/queries/): Contains the queries used for evaluation.

### Finetuning

- [finetuning/](finetuning/): All code for finetuning.
  - [finetuning/dataset.jsonl](finetuning/dataset.jsonl): Main dataset in ShareGPT format.
  - [finetuning/data/](finetuning/data/): Sample datasets used in the datalake for finetuning.
  - [finetuning/langsmith_chat_loader.py](finetuning/langsmith_chat_loader.py): Loads LLM runs from LangSmith and creates datasets for finetuning.

### Data

- [data/](data/): Sample data used for evaluation (e.g., CSVs, JSON files).


