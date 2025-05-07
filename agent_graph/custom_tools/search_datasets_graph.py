from typing import Literal
from sedarapi.workspace import Workspace
from langgraph.graph import StateGraph, START, END
from states.custom_tools.search_datasets_state import SearchDatasetsState
from agents.custom_tools.search_datasets_agent import SearchDatasetsAgent
from ..config import BaseGraphConfig
from ..base_graph import BaseGraph

# This class has a direct dependency on the Workspace class from sedarapi and therefore does not generalize
# We could add a global config parameter to allow custom agents like this or not
# This agent graph could be extended further to include other ways of searching for data,
# instaed of just using the workspace search_datasets method
class SearchDatasetsGraph(BaseGraph):

    def __init__(self, config: BaseGraphConfig, workspace_instance: Workspace):
        super().__init__(config)
        self.workspace_instance = workspace_instance

    def create_graph(self, use_async = False) -> StateGraph:
        graph = StateGraph(SearchDatasetsState)

        graph.add_node(
            "search_datasets_agent",
            lambda state: SearchDatasetsAgent(
                state=state,
                model_config=self.config.default_llm,
                prompt_compression=self.config.prompt_compression
            ).invoke()
        )

        graph.add_node(
            "search_datasets_tool",
            self.search_datasets
        )
        
        graph.add_edge(START, "search_datasets_agent")
        graph.add_conditional_edges("search_datasets_agent", self.should_try_again)
        graph.add_edge("search_datasets_tool", "search_datasets_agent")

        return graph
    
    def should_try_again(self, state: SearchDatasetsState) -> Literal["search_datasets_tool", END]:
        return END if state["query"] == "DONE" else "search_datasets_tool"
    
    def search_datasets(self, state: SearchDatasetsState):
        query = state["query"]
        advanced_search_parameters = state["advanced_search_parameters"]
        search_results = self.workspace_instance.search_datasets(query, advanced_search_parameters)
        state["results"] = state["results"] + search_results
        state["is_initial_search"] = False

        return state
    