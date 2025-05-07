from typing import Any
from ..agent_graph_state import BaseState

class OBDAQueryState(BaseState):
    query: str
    results: dict[str, Any]
    obda_query: str
    sedar_agent_object_cache: dict[str, Any]

def get_initial_obda_query_state(user_query: str, query: str, object_cache: dict[str, Any]) -> OBDAQueryState:
    return {
        "user_query": user_query,
        "messages": [],
        "query": query,
        "obda_query": "",
        "results": {},
        "sedar_agent_object_cache": object_cache
    }