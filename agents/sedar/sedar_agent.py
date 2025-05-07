from ..base_agent import BaseAgent
from states.sedar_agent_state import SedarAgentState
from tools.tool_retrieval import ToolRetriever
from models.config import ModelConfig

class SedarAgent(BaseAgent):

    def __init__(
        self,
        state: SedarAgentState,
        tool_retriever: ToolRetriever,
        model_config: ModelConfig,
        prompt_compression: bool,
        source_node: str
    ):
        super().__init__(state, model_config, prompt_compression, source_node)
        self.tool_retriever = tool_retriever

    def _get_current_query(self):
        decomposed_queries = self.state["decomposed_queries"]
        current_query_index = self.state["current_query_index"]

        return decomposed_queries[current_query_index] if current_query_index < len(decomposed_queries) else ""