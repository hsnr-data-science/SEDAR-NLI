from typing import Literal
from sedarapi.semantic_mapping import SemanticMapping
from sedarapi.mlflow import Experiment
from sedarapi import SedarAPI
from langgraph.graph import StateGraph, START, END
from states.custom_tools.obda_query_state import OBDAQueryState
from agents.custom_tools.obda_query_agent import OBDAQueryAgent
from ..config import BaseGraphConfig
from ..base_graph import BaseGraph

class OBDAQueryGraph(BaseGraph):

    def __init__(
        self,
        config: BaseGraphConfig,
        semantic_mapping_instance: SemanticMapping,
        sedar_api: SedarAPI
    ):
        super().__init__(config)
        self.semantic_mapping_instance = semantic_mapping_instance
        self.sedar_api = sedar_api

    def create_graph(self, use_async = False) -> StateGraph:
        graph = StateGraph(OBDAQueryState)

        graph.add_node(
            "obda_query_agent",
            lambda state: OBDAQueryAgent(
                state=state,
                model_config=self.config.default_llm,
                prompt_compression=self.config.prompt_compression,
                sedar_api=self.sedar_api,
                semantic_mapping=self.semantic_mapping_instance
            ).invoke()
        )

        graph.add_node(
            "obda_query_tool",
            self.execute_obda_query
        )
        
        graph.add_edge(START, "obda_query_agent")
        graph.add_edge("obda_query_agent", "obda_query_tool")
        graph.add_edge("obda_query_tool", END)

        return graph
    
    def execute_obda_query(self, state: OBDAQueryState):
        obda_query = state["obda_query"]

        results = self.semantic_mapping_instance.execute_obda_query(obda_query)
        results = results.get("data", results) if results else results
        results["body"] = results["body"][:10]
        state["results"] = results

        return state
    