import json
from sedarapi import SedarAPI
from langchain_core.messages import SystemMessage, HumanMessage
from prompts.prompts import ml_create_system_prompt, ml_create_prompt_template
from states.custom_tools.ml_create_state import MLCreateState
from ..base_agent import BaseAgent

class MLCreateAgent(BaseAgent):

    def __init__(self, state: MLCreateState, model_config, prompt_compression: bool, sedar_api: SedarAPI, source_node = "ml_create_agent"):
        super().__init__(state, model_config, prompt_compression, source_node)
        self.prompt_template = ml_create_prompt_template
        self.sedar_api = sedar_api
    
    def _get_automl_configurations(self) -> str:
        return json.dumps(self.sedar_api.get_automl_config(), indent=4)

    def invoke(self, prompt: str = None):
        ml_create_prompt = self._get_prompt_template(prompt).format(
            initial_query=self.state["user_query"],
            query=self.state["query"],
            automl_configurations=self._get_automl_configurations(),
            object_cache=self._serialize_json_miminal(self.state["sedar_agent_object_cache"])
        )
        ml_create_prompt = self._compress_prompt_if_needed(ml_create_prompt)

        messages = [
            SystemMessage(content=ml_create_system_prompt),
            *self._get_last_messages(),
            HumanMessage(content=ml_create_prompt)
        ]

        llm = self.get_llm()
        ai_message = llm.invoke(messages)
        ai_message = self._add_metadata_to_message(ai_message)

        automl_parameters = self._parse_llm_response(ai_message.content)

        self.update_state("automl_parameters", automl_parameters)
        self.update_state("messages", [ai_message])

        return self.state