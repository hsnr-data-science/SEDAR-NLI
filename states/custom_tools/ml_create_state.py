from typing import Any
from sedarapi.mlflow import Notebook
from ..agent_graph_state import BaseState

class MLCreateState(BaseState):
    query: str
    automl_parameters: dict
    results: Notebook
    sedar_agent_object_cache: dict[str, Any]

def get_initial_ml_create_state(user_query: str, query: str, object_cache: dict[str, Any]) -> MLCreateState:
    return {
        "user_query": user_query,
        "messages": [],
        "query": query,
        "automl_parameters": {},
        "results": None,
        "sedar_agent_object_cache": object_cache
    }