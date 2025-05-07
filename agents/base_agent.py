from abc import ABC, abstractmethod
import json
from typing import Optional
from langchain_core.messages import BaseMessage, ToolMessage, AIMessage
from states.agent_graph_state import BaseState
from models.models import get_model
from models.config import ModelConfig
from prompts.prompt_compress import PromptCompressor
from utils.custom_json_encoder import MinimalEncoder, ExtendedEncoder
from utils import utils

class BaseAgent(ABC):
    
    def __init__(self, state: BaseState, model_config: ModelConfig, prompt_compression: bool, source_node: str):
        self.state = state
        self.model_config = model_config
        self.prompt_compression = prompt_compression
        self.prompt_template = None
        self.prompt_template_compressed = None
        self.source_node = source_node

    def _get_class_info(self, current_instance):
        class_info = f"{current_instance.__class__.__name__}\n{current_instance.__doc__}\n"
        return class_info

    def _parse_llm_response(self, response: str) -> dict:
        return utils.load_json(response)
    
    def _serialize_json_miminal(self, obj) -> str:
        """
        Serialize an object to JSON using the custom minimal encoder.
        For example a workspace object will be serialized as "Workspace(id=..., name=...)".
        """
        return json.dumps(obj, cls=MinimalEncoder)
    
    def _serialize_json(self, obj) -> str:
        """
        Serialize an object to JSON using the custom extended encoder.
        For example a workspace object will be serialized as "{\"id\": ..., \"name\": ..., ...}".
        """
        return json.dumps(obj, cls=ExtendedEncoder, max_depth=2)
    
    def _get_last_messages(self, agent_messages_to_exclude: list[str] = []) -> list[BaseMessage]:
        """
        Get the last messages in the state, excluding those specified in the agent_messages_to_exclude list.
        
        Args:
            agent_messages_to_exclude (list[str]): A list of message keys to exclude from the messages.
        
        Returns:
            list[BaseMessage]: A list of messages excluding those specified in the agent_messages_to_exclude list.
        """
        all_messages = self.state["messages"]

        # Always exclude tool and code execution messages since we pass their results into the prompts already
        all_messages = [msg for msg in all_messages if not isinstance(msg, ToolMessage)]
        all_messages = [msg for msg in all_messages if msg.source_node not in ["sedar_tool", "code_executor", "tool_agent"]]

        ids_to_filter = {
            msg.id for key in agent_messages_to_exclude for msg in self.state.get(key, [])
        }

        # Take only the last 10
        last_messages = [msg for msg in all_messages if msg.id not in ids_to_filter][-10:]

        # We can't start with a tool message without preceeding tool call (openAI API limitation)
        if len(last_messages) > 0 and isinstance(last_messages[0], ToolMessage):
            last_messages = last_messages[1:]

        # And we can't finish with a tool call without a subsequent tool message (Gemini API limitation)
        if len(last_messages) > 0 and isinstance(last_messages[-1], AIMessage) and last_messages[-1].tool_calls:
            last_messages = last_messages[:-1]

        return last_messages
    
    def _get_prompt_template(self, custom_template: str) -> str:
        if custom_template:
            return custom_template

        if self.prompt_compression and self.prompt_template_compressed:
            return self.prompt_template_compressed
        
        return self.prompt_template
    
    def _compress_prompt_if_needed(self, prompt: str) -> str:
        if self.prompt_compression and self.prompt_template_compressed:
            return PromptCompressor().compress_prompt(prompt)
        return prompt
    
    def _add_metadata_to_message(self, message: BaseMessage, query_index: Optional[int] = None) -> BaseMessage:
        message.source_node = self.source_node
        if query_index is not None:
            message.query_index = query_index
        return message

    def get_llm(self, tools=[]):
        return get_model(model_config=self.model_config, tools=tools)

    def update_state(self, key, value):
        self.state = {**self.state, key: value}

    @abstractmethod
    def invoke(self):
        pass