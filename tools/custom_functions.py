import asyncio
from typing import Optional
from langchain_core.runnables import RunnableConfig
from langchain_core.tools import ToolException
from langgraph.errors import GraphRecursionError
from sedarapi.workspace import Workspace
from sedarapi.mlflow import Experiment
from sedarapi.ontology import Ontology
from sedarapi.semantic_model import SemanticModel
from sedarapi.semantic_mapping import SemanticMapping
from agent_graph.custom_tools.search_datasets_graph import SearchDatasetsGraph
from agent_graph.custom_tools.create_dataset_graph import CreateDatasetGraph
from agent_graph.custom_tools.ml_create_agent_graph import MLCreateGraph
from agent_graph.custom_tools.semantic_labeling_graph import SemanticLabelingGraph
from agent_graph.custom_tools.obda_query_agent_graph import OBDAQueryGraph
from agent_graph.config import BaseGraphConfig
from models.config import ModelConfig, Servers, Models, Embeddings
from states.custom_tools.search_datasets_state import get_initial_search_state
from states.custom_tools.create_dataset_state import get_initial_create_dataset_state
from states.custom_tools.ml_create_state import get_initial_ml_create_state
from states.custom_tools.semantic_labeling_state import (
    get_initial_semantic_labeling_state,
)
from states.custom_tools.obda_query_state import get_initial_obda_query_state
from cache.cacheable import CacheableRegistry
from utils.utils import is_async_context

custom_function_config = BaseGraphConfig(
    default_llm=ModelConfig(server=Servers.AZURE_OPENAI, model=Models.GPT_4O),
    embedding_model=ModelConfig(
        server=Servers.OLLAMA_HSNR,
        model=Embeddings.NOMIC_EMBED_TEXT,
        embedding_size=768,
    ),
    full_doc_strings=False,
    prompt_compression=False,
    human_confirmation=False,
)


# We need to make sure the method has a different name than the original method in the Workspace class.
def datasets_search(self, query: str, **kwargs: Optional[dict]):
    """
    Searches for datasets in the SEDAR system inside the specified workspace.
    Use this to find datasets in more general terms based on some search query.
    Prefer this to find a dataset based on the title.
    The query can be in natural language.

    Args:
        query (str): A typical query string for a text based search.

    Returns:
        list[Dataset]: A list of Dataset instances representing each favorite dataset in the workspace.
        The content of each dataset can be accessed using the `.content` attribute.

    Raises:
        Exception: If the dataset search fails.

    Description:
        This method searches for datasets within the specified workspace.
        It sends a POST request to the '/api/v1/workspace/{workspace_id}/search' endpoint with the provided details and checks the response.

    Notes:
        - The dataset search will only find published datasets.
    """
    graph = SearchDatasetsGraph(config=custom_function_config, workspace_instance=self)
    workflow = graph.compile_workflow()
    # graph.generate_graph_image(workflow, "search_datasets_graph.png")
    initial_state = get_initial_search_state(kwargs.get("initial_query", ""), query)
    final_state = initial_state
    try:
        if is_async_context():
            final_state = asyncio.get_running_loop().run_until_complete(
                workflow.ainvoke(
                    initial_state, config=RunnableConfig(recursion_limit=6)
                )
            )
        else:
            final_state = workflow.invoke(
                initial_state, config=RunnableConfig(recursion_limit=6)
            )
    except GraphRecursionError as e:
        return final_state["results"]
    except Exception as e:
        raise ToolException(f"Error in datasets_search tool: {e}")

    return final_state["results"]


# We need to make sure the method has a different name than the original method in the Workspace class.
def dataset_create(self, user_query: str, filename: str, **kwargs: Optional[dict]):
    """
    Creates a dataset in the SEDAR system inside the specified workspace.
    Use this to create a new dataset based on a file. This method is the first step before ingesting and publishing the dataset.
    The user query should contain the name of the dataset and other details about the creation.

    Args:
        user_query (str): The current user query containing all details about the dataset creation (e.g. name, other parameters given by the user)
        filename (str): The exact filename of the file that is to be uploaded.

    Returns:
        Dataset: An instance of the Dataset class representing the newly created dataset.

    Raises:
        Exception: If the dataset creation fails.
    """
    graph = CreateDatasetGraph(config=custom_function_config, workspace_instance=self)
    workflow = graph.compile_workflow()
    # graph.generate_graph_image(workflow, "create_dataset_graph.png")
    initial_state = get_initial_create_dataset_state(user_query, filename)
    try:
        if is_async_context():
            final_state = asyncio.get_running_loop().run_until_complete(
                workflow.ainvoke(initial_state)
            )
        else:
            final_state = workflow.invoke(initial_state)

        return final_state["results"]
    except Exception as e:
        raise ToolException(f"Error in dataset_create tool: {e}")


# We need to make sure the method has a different name than the original method in the Workspace class.
def create_ml_notebook(self, query: str, **kwargs: Optional[dict]):
    """
    Creates a jupyter notebook for machine learning for an existing experiment.
    This uses AutoML to generate a notebook based on parameters.
    Use this method to let an agent create the AutoML run based on the user query.

    Args:
        query (str): User query containing the details about the machine learning experiment to be created.

    Returns:
        Notebook: An instance of the Notebook class representing the newly created notebook.

    Raises:
        Exception: If the notebook creation fails.
    """
    sedar_agent_object_cache = kwargs.get("object_cache", {})
    sedar_api = kwargs.get("sedar_api", None)
    initial_query = kwargs.get("initial_query", "")

    graph = MLCreateGraph(
        config=custom_function_config, experiment_instance=self, sedar_api=sedar_api
    )
    workflow = graph.compile_workflow()
    # graph.generate_graph_image(workflow, "ml_create_agent_graph.png")
    initial_state = get_initial_ml_create_state(
        user_query=initial_query, query=query, object_cache=sedar_agent_object_cache
    )

    final_state = initial_state
    try:
        if is_async_context():
            final_state = asyncio.get_running_loop().run_until_complete(
                workflow.ainvoke(
                    initial_state, config=RunnableConfig(recursion_limit=5)
                )
            )
        else:
            final_state = workflow.invoke(
                initial_state, config=RunnableConfig(recursion_limit=5)
            )

    except GraphRecursionError as e:
        return final_state["results"]
    except Exception as e:
        raise ToolException(f"Error in create_ml_notebook tool: {e}")

    return final_state["results"]


def label_modeling_attributes(self, ontology: Ontology, **kwargs: Optional[dict]):
    """
    This function can be used for semantic labeling of datasets. A semantic modeling is required for this.
    This function will label the attributes or columns of the datasets inside a semantic model.
    The semantic model serves as basis for OBDA and can later be used to execute SPARQL queries, if the modeling is turned into a mapping.

    Args:
        ontology (Ontology): The ontology instance to use for labeling the attributes.

    Returns:
        bool: True if the labeling was successful, False otherwise.

    Raises:
        Exception: If the labeling fails.
    """
    object_cache = kwargs.get("object_cache", {})
    workspace = None

    for key, value in object_cache.items():
        if key.startswith("_WORKSPACE_"):
            workspace = value

    graph = SemanticLabelingGraph(config=custom_function_config, modeling_instance=self)
    workflow = graph.compile_workflow()
    # graph.generate_graph_image(workflow, "semantic_labeling_graph.png")

    initial_state = get_initial_semantic_labeling_state(ontology, workspace)

    try:
        if is_async_context():
            asyncio.get_running_loop().run_until_complete(
                workflow.ainvoke(initial_state)
            )
        else:
            workflow.invoke(initial_state)
        return True
    except Exception as e:
        raise ToolException(f"Error in label_modeling_attributes tool: {e}")


def execute_sparql_query(self, query: str, **kwargs: Optional[dict]):
    """
    Executes a SPARQL query on a semantic mapping.
    A mapping can be created from a semantic model.
    Use this method to query heterogeneous data sources or join multiple datasets and gather combined information.

    Args:
        query (str): User query in natural language containing the details about the query to be created.

    Raises:
        Exception: If the sparql query execution or generation fails.
    """
    sedar_agent_object_cache = kwargs.get("object_cache", {})
    sedar_api = kwargs.get("sedar_api", None)
    initial_query = kwargs.get("initial_query", "")

    graph = OBDAQueryGraph(
        config=custom_function_config,
        semantic_mapping_instance=self,
        sedar_api=sedar_api,
    )
    workflow = graph.compile_workflow()
    # graph.generate_graph_image(workflow, "obda_query_agent_graph.png")
    initial_state = get_initial_obda_query_state(
        user_query=initial_query, query=query, object_cache=sedar_agent_object_cache
    )

    final_state = initial_state
    try:
        if is_async_context():
            final_state = asyncio.get_running_loop().run_until_complete(
                workflow.ainvoke(
                    initial_state, config=RunnableConfig(recursion_limit=5)
                )
            )
        else:
            final_state = workflow.invoke(
                initial_state, config=RunnableConfig(recursion_limit=5)
            )

    except GraphRecursionError as e:
        return final_state["results"]
    except Exception as e:
        raise ToolException(f"Error in execute_sparql_query tool: {e}")

    return final_state["results"]


def register_methods():
    CacheableRegistry.register_method(
        target_class=Workspace,
        method_name="datasets_search",
        method_func=datasets_search,
    )
    CacheableRegistry.register_method(
        target_class=Workspace, method_name="dataset_create", method_func=dataset_create
    )
    CacheableRegistry.register_method(
        target_class=Experiment,
        method_name="create_ml_notebook",
        method_func=create_ml_notebook,
    )
    CacheableRegistry.register_method(
        target_class=SemanticModel,
        method_name="label_modeling_attributes",
        method_func=label_modeling_attributes,
    )
    CacheableRegistry.register_method(
        target_class=SemanticMapping,
        method_name="execute_sparql_query",
        method_func=execute_sparql_query,
    )
