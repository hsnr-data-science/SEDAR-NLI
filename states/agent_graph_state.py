from typing import Annotated, Optional
from langgraph.graph.message import add_messages
from langchain_core.messages import HumanMessage, AIMessage
from sedarapi import SedarAPI
from sedarapi.workspace import Workspace
from .sedar_agent_state import SedarAgentState
from .base_state import BaseState
from consts import SEDAR_BASE_URL
from utils.utils import generate_short_uuid

# This is the main agent state
class AgentGraphState(BaseState):
    query_decompose_agent_messages: Annotated[list, add_messages]
    final_response: str
    decomposed_queries: list[str]
    sedar_agent_state: SedarAgentState
    has_errored: bool

def get_initial_state(
    user_query: str,
    jwt: Optional[str] = None,
    jupyter_token: Optional[str] = None,
    workspace_id: Optional[str] = None
) -> AgentGraphState:
    sedar = SedarAPI(base_url=SEDAR_BASE_URL)

    if jwt and jupyter_token:
        sedar.login_existing_tokens(jwt, jupyter_token)
    else:
        sedar.login_gitlab()

    workspace = sedar.get_workspace(workspace_id) if workspace_id else sedar.get_default_workspace()
    sedar_agent_state = {
        "object_cache": {
            f"_SEDARAPI_{generate_short_uuid()}": sedar,
            f"_WORKSPACE_{generate_short_uuid()}": workspace
        }
    }

    return {
        "user_query": user_query,
        "messages": [],
        "query_decompose_agent_messages": [],
        "final_response": "",
        "decomposed_queries": [],
        "sedar_agent_state": sedar_agent_state,
        "has_errored": False
    }

def reset_state(state: AgentGraphState, user_query: str) -> AgentGraphState:
    messages = []
    if state["user_query"] and state["final_response"]:
        last_human_message = HumanMessage(content=state["user_query"])
        last_ai_message = AIMessage(content=state["final_response"])
        messages = [last_human_message, last_ai_message]

    return {
        "user_query": user_query,
        "messages": messages,
        "query_decompose_agent_messages": [],
        "final_response": "",
        "decomposed_queries": [],
        "sedar_agent_state": state["sedar_agent_state"],
        "has_errored": False
    }