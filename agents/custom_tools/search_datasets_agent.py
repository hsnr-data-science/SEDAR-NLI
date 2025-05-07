from langchain_core.messages import SystemMessage, HumanMessage
from prompts.prompts import search_datasets_system_prompt, search_datasets_prompt_template
from states.custom_tools.search_datasets_state import SearchDatasetsState
from ..base_agent import BaseAgent

class SearchDatasetsAgent(BaseAgent):

    def __init__(self, state: SearchDatasetsState, model_config, prompt_compression: bool, source_node = "search_datasets_agent"):
        super().__init__(state, model_config, prompt_compression, source_node)
        self.prompt_template = search_datasets_prompt_template
    
    def _get_last_search_results(self) -> str:
        last_query = self.state["query"]
        results = self.state["results"]
        if self.state["is_initial_search"]:
            return ""
        if len(results) == 0:
            return f"Here are the results from your last search:\nLast search query: {last_query}\nResults: None"
        else:
            return f"Here are the results from your last search:\nLast search query: {last_query}\nResults: {self._serialize_json_miminal(results)}"

    def invoke(self, prompt=None):
        initial_query = self.state["user_query"]
        query = self.state["query"]
        last_search_results = self._get_last_search_results()

        search_prompt = self._get_prompt_template(prompt).format(
            initial_query=initial_query,
            query=query,
            last_search_results=last_search_results
        )
        search_prompt = self._compress_prompt_if_needed(search_prompt)

        messages = [
            SystemMessage(content=search_datasets_system_prompt),
            *self._get_last_messages(),
            HumanMessage(content=search_prompt)
        ]

        llm = self.get_llm()
        ai_message = llm.invoke(messages)
        ai_message = self._add_metadata_to_message(ai_message)

        search_datasets_response = {}

        if "DONE" in ai_message.content:
            search_datasets_response = {"query": "DONE", "advanced_search_parameters": {}}
        else:
            search_datasets_response = self._parse_llm_response(ai_message.content)

        self.update_state("query", search_datasets_response["query"])
        self.update_state("advanced_search_parameters", search_datasets_response["advanced_search_parameters"])
        self.update_state("messages", [ai_message])

        return self.state
        