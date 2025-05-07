from typing import Annotated
from typing_extensions import TypedDict
from langgraph.graph.message import add_messages

class BaseState(TypedDict):
    user_query: str
    messages: Annotated[list, add_messages]