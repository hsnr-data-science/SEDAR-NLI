from langchain_core.messages import ToolMessage
from typing import Any


class SedarToolMessage(ToolMessage):
    raw_output: Any
    source_node: str

    def __init__(self, content: str, raw_content: Any, source_node: str, **kwargs):
        super().__init__(
            content=content, raw_output=raw_content, source_node=source_node, **kwargs
        )
        self.raw_output = raw_content
        source_node = source_node
