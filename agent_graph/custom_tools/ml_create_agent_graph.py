from typing import Literal
from sedarapi.mlflow import Experiment
from sedarapi import SedarAPI
from langgraph.graph import StateGraph, START, END
from states.custom_tools.ml_create_state import MLCreateState
from agents.custom_tools.ml_create_agent import MLCreateAgent
from ..config import BaseGraphConfig
from ..base_graph import BaseGraph

class MLCreateGraph(BaseGraph):

    def __init__(
        self,
        config: BaseGraphConfig,
        experiment_instance: Experiment,
        sedar_api: SedarAPI
    ):
        super().__init__(config)
        self.experiment_instance = experiment_instance
        self.sedar_api = sedar_api

    def create_graph(self, use_async = False) -> StateGraph:
        graph = StateGraph(MLCreateState)

        graph.add_node(
            "ml_create_agent",
            lambda state: MLCreateAgent(
                state=state,
                model_config=self.config.default_llm,
                prompt_compression=self.config.prompt_compression,
                sedar_api=self.sedar_api
            ).invoke()
        )

        graph.add_node(
            "ml_create_tool",
            self.create_ml
        )
        
        graph.add_edge(START, "ml_create_agent")
        graph.add_edge("ml_create_agent", "ml_create_tool")
        graph.add_edge("ml_create_tool", END)

        return graph
    
    def create_ml(self, state: MLCreateState):
        automl_parameters = state["automl_parameters"]
        sedar_agent_object_cache = state["sedar_agent_object_cache"]
        automl_parameters["datasets"] = [sedar_agent_object_cache[dataset_cache_key] for dataset_cache_key in automl_parameters["datasets"]]
        new_notebook = self.experiment_instance.create_automl_run(**automl_parameters)
        state["results"] = new_notebook

        return state
    