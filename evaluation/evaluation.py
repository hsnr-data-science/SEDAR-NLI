from langsmith import evaluate, Client
from langchain_core.rate_limiters import InMemoryRateLimiter
from main import setup
from models.config import ModelConfig, Servers, Models
from .llm_judge import LLMJudgeEvaluator
from .node_sequences import NodeSequenceEvaluator
from .tool_sequences import ToolSequenceEvaluator
from .action_execution import ActionExecutionEvaluator

llm_model = Models.FINETUNED_LLAMA3_3
llm_server = Servers.OLLAMA_HSNR
splits = [
    "Get all datasets",
    "Get All Versions",
    "Get Lineage",
    "Get Linked Datasets",
    "Get Row Count",
    "Get Stats",
    "Get Dataset Description",
    "List Columns",
    "List Tags",
    "List Users",
    "Search Chemical Datasets",
    "Workspace Title",
    "Find Related Datasets",
    "Query Semantic Mapping",
    "Versions And Deltas",
    "Labeling and Semantic Mapping",
    "Describe Dataset",
    "Create semantic model",
    "Create Regression",
    "Create Binary Classification",
    "Create Ontology",
    "Create And Ingest",
    "Create Dataset",
    "Create Dataset And Find Tag",
    "Upload Ingest Two",
    "Delete Dataset",  # After each execution, the dataset has to be recreated
    "Update Dataset Title",  # After each execution, the dataset has to be renamed
]

for split in splits:
    workflow = setup(
        ModelConfig(
            server=llm_server,
            model=llm_model,
            temperature=0.1,
            # reasoning_effort="high",
            # rate_limiter=InMemoryRateLimiter(requests_per_second=0.25)
            # rate_limiter=InMemoryRateLimiter(requests_per_second=1)
        ),
        prompt_compression=True,
    )
    client = Client()
    dataset = client.list_examples(dataset_name="queries-ground-truth", splits=[split])
    llm_judge_evaluator = LLMJudgeEvaluator(
        model_config=ModelConfig(server=Servers.AZURE_OPENAI, model=Models.GPT_4O)
    )
    node_sequence_evaluator = NodeSequenceEvaluator(metric_prefix="nodes")
    tool_sequence_evaluator = ToolSequenceEvaluator(metric_prefix="tools")
    action_execution_evaluator = ActionExecutionEvaluator()

    evaluate(
        workflow.invoke,
        data=dataset,
        evaluators=[
            *node_sequence_evaluator.get_all_evals(),
            *tool_sequence_evaluator.get_all_evals(),
            llm_judge_evaluator.evaluate_final_answer_correct,
            action_execution_evaluator.evaluate_pass_at_1,
            action_execution_evaluator.evaluate_pass_at_2,
        ],
        experiment_prefix=f"{llm_model}-PC-{split.replace(' ', '-')}",
    )
