from langchain_core.messages import SystemMessage, HumanMessage
from tools.sedar_tool_message import SedarToolMessage
from prompts.prompts import final_response_system_prompt, final_response_prompt_template
from .main_agent import MainAgent

class FinalResponseAgent(MainAgent):

    def __init__(self, state, tool_retriever, model_config, prompt_compression: bool, source_node = "final_response_agent"):
        super().__init__(state, tool_retriever, model_config, prompt_compression, source_node)
        self.prompt_template = final_response_prompt_template

    def _get_synthesize_responses(self):
        return "\n\n".join([message.content for message in self.state["sedar_agent_state"]["synthesize_agent_messages"]])

    def invoke(self, prompt=None):
        user_query = self.state["user_query"]
        synthesize_responses = self._get_synthesize_responses()

        final_response_prompt = self._get_prompt_template(prompt).format(query=user_query, agent_responses=synthesize_responses)
        final_response_prompt = self._compress_prompt_if_needed(final_response_prompt)

        messages = [
            SystemMessage(content=final_response_system_prompt),
            *self._get_last_messages(),
            HumanMessage(content=final_response_prompt)
        ]

        llm = self.get_llm()
        ai_message = llm.invoke(messages)
        ai_message = self._add_metadata_to_message(ai_message)

        final_response = ai_message.content

        self.update_state("messages", [ai_message])
        self.update_state("final_response", final_response)

        return self.state