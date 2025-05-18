from sedarapi.workspace import Workspace
from langgraph.graph import StateGraph, START, END
from states.custom_tools.create_dataset_state import CreateDatasetState
from agents.custom_tools.create_dataset_agent import CreateDatasetAgent
from ..config import BaseGraphConfig
from ..base_graph import BaseGraph


# This class has a direct dependency on the Workspace class from sedarapi and therefore does not generalize
# We could add a global config parameter to allow custom agents like this or not
class CreateDatasetGraph(BaseGraph):
    def __init__(self, config: BaseGraphConfig, workspace_instance: Workspace):
        super().__init__(config)
        self.workspace_instance = workspace_instance

    def create_graph(self, use_async=False) -> StateGraph:
        graph = StateGraph(CreateDatasetState)

        graph.add_node(
            "create_dataset_agent",
            lambda state: CreateDatasetAgent(
                state=state,
                model_config=self.config.default_llm,
                prompt_compression=self.config.prompt_compression,
            ).invoke(),
        )

        graph.add_node("create_dataset_tool", self.create_dataset)

        graph.add_edge(START, "create_dataset_agent")
        graph.add_edge("create_dataset_agent", "create_dataset_tool")
        graph.add_edge("create_dataset_tool", END)

        return graph

    def create_dataset(self, state: CreateDatasetState):
        filename = state["filename"]
        datasource_definition = state["datasource_definition"]

        # NOTE: THIS HAS TO BE ./.files/ FOR THE CHATBOT OR ./data/ FOR THE EVALUATION
        dataset = self.workspace_instance.create_dataset(
            datasource_definition, f"./.files/{filename}"
        )
        state["results"] = dataset

        return state
