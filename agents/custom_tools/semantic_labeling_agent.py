import json
from langchain_core.messages import SystemMessage, HumanMessage
from sedarapi.dataset import Dataset
from sedarapi.ontology import Ontology
from prompts.prompts import semantic_labeling_system_prompt, semantic_labeling_prompt_template
from states.custom_tools.semantic_labeling_state import SemanticLabelingState
from ..base_agent import BaseAgent

class SemanticLabelingAgent(BaseAgent):

    def __init__(self, state: SemanticLabelingState, model_config, prompt_compression: bool, source_node = "semantic_labeling_agent"):
        super().__init__(state, model_config, prompt_compression, source_node)
        self.prompt_template = semantic_labeling_prompt_template
    
    def _get_dataset_preview(self) -> str:
        current_dataset: Dataset = self.state["current_dataset"]
        dataset_preview_json = current_dataset.get_preview_json()
        dataset_primary_key = next((attribute for attribute in current_dataset.get_all_attributes() if attribute.is_pk), None)

        # Remove primary as it doesn't need to be labeled
        if dataset_primary_key.name in dataset_preview_json["header"]:
            dataset_preview_json["header"].remove(dataset_primary_key.name)

        for entry in dataset_preview_json["body"]:
            entry.pop(dataset_primary_key.name, None)

        return json.dumps(dataset_preview_json, indent=4)
    
    def _get_all_ontology_annotations(self) -> str:
        ontology: Ontology = self.state["ontology"]
        return "\n".join([annotation.title for annotation in ontology.get_all_classes()])

    def invoke(self, prompt: str = None):
        semantic_labeling_prompt = self._get_prompt_template(prompt).format(
            dataset_preview=self._get_dataset_preview(),
            available_labels=self._get_all_ontology_annotations()
        )

        semantic_labeling_prompt = self._compress_prompt_if_needed(semantic_labeling_prompt)

        messages = [
            SystemMessage(content=semantic_labeling_system_prompt),
            HumanMessage(content=semantic_labeling_prompt)
        ]

        llm = self.get_llm()
        ai_message = llm.invoke(messages)
        ai_message = self._add_metadata_to_message(ai_message)

        assigned_labels = self._parse_llm_response(ai_message.content)

        self.update_state("last_assigned_labels", assigned_labels)
        self.update_state("messages", [ai_message])

        return self.state