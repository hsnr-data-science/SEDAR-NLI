from ..base_agent import BaseAgent
from states.agent_graph_state import AgentGraphState
from tools.tool_retrieval import ToolRetriever
from models.config import ModelConfig

class MainAgent(BaseAgent):

    def __init__(
        self,
        state: AgentGraphState,
        tool_retriever: ToolRetriever,
        model_config: ModelConfig,
        prompt_compression: bool,
        source_node: str
    ):
        super().__init__(state, model_config, prompt_compression, source_node)
        self.tool_retriever = tool_retriever