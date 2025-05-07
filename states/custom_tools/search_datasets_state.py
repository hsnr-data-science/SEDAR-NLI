from ..agent_graph_state import BaseState

class SearchDatasetsState(BaseState):
    query: str
    advanced_search_parameters: dict
    results: list
    is_initial_search: bool

def get_initial_search_state(user_query: str, query: str) -> SearchDatasetsState:
    return {
        "user_query": user_query,
        "messages": [],
        "query": query,
        "advanced_search_parameters": {},
        "results": [],
        "is_initial_search": True
    }