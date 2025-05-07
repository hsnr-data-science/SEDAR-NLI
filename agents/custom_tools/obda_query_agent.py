from sedarapi import SedarAPI
from sedarapi.semantic_mapping import SemanticMapping
from langchain_core.messages import SystemMessage, HumanMessage
from prompts.prompts import obda_query_system_prompt, obda_query_prompt_template
from states.custom_tools.obda_query_state import OBDAQueryState
from utils.utils import remove_json_code_block_markers
from ..base_agent import BaseAgent

class OBDAQueryAgent(BaseAgent):

    def __init__(
            self,
            state: OBDAQueryState,
            model_config,
            prompt_compression: bool,
            sedar_api: SedarAPI,
            semantic_mapping: SemanticMapping,
            source_node = "obda_query_agent"):
        super().__init__(state, model_config, prompt_compression, source_node)
        self.prompt_template = obda_query_prompt_template
        self.sedar_api = sedar_api
        self.semantic_mapping = semantic_mapping

    def invoke(self, prompt: str = None):
        obda_query_prompt = self._get_prompt_template(prompt).format(
            initial_query=self.state["user_query"],
            # query=self.state["query"],
            mapping_file=self.semantic_mapping.mappings_file
        )

        obda_query_prompt = self._compress_prompt_if_needed(obda_query_prompt)

        messages = [
            SystemMessage(content=obda_query_system_prompt),
            *self._get_last_messages(),
            HumanMessage(content=obda_query_prompt)
        ]

        llm = self.get_llm()
        ai_message = llm.invoke(messages)
        ai_message = self._add_metadata_to_message(ai_message)

        obda_query = remove_json_code_block_markers(ai_message.content)

        self.update_state("obda_query", obda_query)
        self.update_state("messages", [ai_message])

        return self.state