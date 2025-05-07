import chainlit as cl
from typing import Literal
from sedarapi.semantic_model import SemanticModel
from langgraph.graph import StateGraph, START, END
from states.custom_tools.semantic_labeling_state import SemanticLabelingState
from agents.custom_tools.semantic_labeling_agent import SemanticLabelingAgent
from ..config import BaseGraphConfig
from ..base_graph import BaseGraph

# This class has a direct dependency on classes from the sedarapi and therefore does not generalize
# We could add a global config parameter to allow custom agents like this or not
class SemanticLabelingGraph(BaseGraph):

    def __init__(self, config: BaseGraphConfig, modeling_instance: SemanticModel):
        super().__init__(config)
        self.modeling_instance = modeling_instance

    def create_graph(self, use_async = False) -> StateGraph:
        graph = StateGraph(SemanticLabelingState)

        graph.add_node(
            "get_dataset_to_label",
            self.get_dataset_to_label
        )

        graph.add_node(
            "semantic_labeling_agent",
            lambda state: SemanticLabelingAgent(
                state=state,
                model_config=self.config.default_llm,
                prompt_compression=self.config.prompt_compression
            ).invoke()
        )

        graph.add_node(
            "label_modeling_attributes",
            self.label_modeling_attributes
        )
        
        graph.add_edge(START, "get_dataset_to_label")
        graph.add_edge("get_dataset_to_label", "semantic_labeling_agent")

        if use_async:
            graph.add_conditional_edges(
                "semantic_labeling_agent",
                self.aconfirm_labels
            )
        else:
            graph.add_edge("semantic_labeling_agent", "label_modeling_attributes")

        graph.add_conditional_edges(
            "label_modeling_attributes",
            self.has_more_datasets
        )

        return graph
    
    async def aconfirm_labels(self, state: SemanticLabelingState) -> Literal["label_modeling_attributes", "semantic_labeling_agent"]:
        assigned_labels = state["last_assigned_labels"]

        content = "Please confirm the following labels:\n\n"
        content += "| Columns | Labels |\n"
        content += "|---------|--------|\n"
        for attribute_name, label_name in assigned_labels.items():
            content += f"| {attribute_name} | {label_name} |\n"

        res = await cl.AskActionMessage(
            content=content,
            actions=[
                cl.Action(name="yes", payload={"value": "y"}, label="Yes ✅"),
                cl.Action(name="no", payload={"value": "n"}, label="No ❌")
            ]
        ).send()

        if res.get("payload").get("value") == "n":
            return "semantic_labeling_agent"
        return "label_modeling_attributes"

    def get_dataset_to_label(self, state: SemanticLabelingState):
        dataset_id = self.modeling_instance.dataset_ids[state["current_dataset_index"]]
        state["current_dataset"] = state["current_workspace"].get_dataset(dataset_id)

        return state
    
    def label_modeling_attributes(self, state: SemanticLabelingState):
        dataset = state["current_dataset"]
        assigned_labels = state["last_assigned_labels"]

        all_attributes = dataset.get_all_attributes()
        all_annotations = state["ontology"].get_all_classes()

        for attribute_name, label_name in assigned_labels.items():
            attribute = next((attribute for attribute in all_attributes if attribute.name == attribute_name), None)
            label = next((annotation for annotation in all_annotations if annotation.title == label_name), None)

            if not attribute or not label:
                continue

            self.modeling_instance.add_semantic_label_to_attribute(dataset, attribute, label)

        state["current_dataset_index"] += 1

        return state
    
    def has_more_datasets(self, state: SemanticLabelingState) -> Literal["get_dataset_to_label", END]:
        if state["current_dataset_index"] < len(self.modeling_instance.dataset_ids):
            return "get_dataset_to_label"
        return END
