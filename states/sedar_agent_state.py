from langgraph.graph.message import add_messages
from typing import Any, Annotated
from .base_state import BaseState
from sedarapi import SedarAPI
from cache.cacheable import CacheableRegistry
from consts import SEDAR_BASE_URL
from utils.utils import generate_short_uuid

class SedarAgentState(BaseState):
    current_query_index: int
    current_query_number_of_tries: int
    current_query: str
    next_action: str
    current_instance: Any
    sedar_api: SedarAPI
    code_agent_messages: Annotated[list, add_messages]
    manager_agent_messages: Annotated[list, add_messages]
    tool_agent_messages: Annotated[list, add_messages]
    tool_execution_messages: Annotated[list, add_messages]
    code_execution_messages: Annotated[list, add_messages]
    synthesize_agent_messages: Annotated[list, add_messages]
    object_cache: dict[str, Any]
    decomposed_queries: list[str]

def get_sedar_api_from_cache(object_cache: dict[str, Any]) -> SedarAPI:
    for _, value in object_cache.items():
        if isinstance(value, SedarAPI):
            return value
    return None

def get_state(user_query: str, decomposed_queries: list[str], sedar_agent_state: SedarAgentState) -> SedarAgentState:

    # Workaround for the evaluation of the system
    if not sedar_agent_state or any(isinstance(value, str) for value in sedar_agent_state["object_cache"].values()):
        sedar = SedarAPI(base_url=SEDAR_BASE_URL)
        sedar.login_gitlab()

        sedar_agent_state = {
            "object_cache": {
                f"_SEDARAPI_{generate_short_uuid()}": sedar,
                f"_WORKSPACE_{generate_short_uuid()}": sedar.get_default_workspace()
            }
        }

    return {
        "user_query": user_query,
        "current_query_index": -1,
        "current_query_number_of_tries": 0,
        "current_query": "",
        "messages": [],
        "code_agent_messages": [],
        "manager_agent_messages": [],
        "tool_agent_messages": [],
        "synthesize_agent_messages": [],
        "tool_execution_messages": [],
        "code_execution_messages": [],
        "next_action": "",
        "current_instance": None,
        "sedar_api": get_sedar_api_from_cache(sedar_agent_state["object_cache"]),
        "object_cache": sedar_agent_state["object_cache"],
        "decomposed_queries": decomposed_queries,
    }

def get_tool_objects(state) -> dict[str, Any]:
    """
    Used to get the objects from the cache that can be used for tool-calling.
    """
    return {key: value for key, value in state["object_cache"].items() if CacheableRegistry.is_cacheable(value)}

def get_remaining_objects(state) -> dict[str, Any]:
    """-
    Used to get the objects from the cache that are not cacheable (not usable for tool-calling).
    """
    return {key: value for key, value in state["object_cache"].items() if not CacheableRegistry.is_cacheable(value)}