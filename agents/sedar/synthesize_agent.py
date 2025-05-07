from langchain_core.messages import SystemMessage, HumanMessage
from prompts.prompts import synthesize_system_prompt, synthesize_prompt_template, synthesize_prompt_template_compressed 
from .sedar_agent import SedarAgent

class SynthesizeAgent(SedarAgent):

    def __init__(self, state, tool_retriever, model_config, prompt_compression: bool, source_node = "synthesize_agent"):
        super().__init__(state, tool_retriever, model_config, prompt_compression, source_node)
        self.prompt_template = synthesize_prompt_template
        self.prompt_template_compressed = synthesize_prompt_template_compressed

    def _get_last_messages_content(self):
        last_messages_content = ""

        if len(self.state["messages"]) >= 2:
            last_messages_content += f"The last action:\n{self.state['messages'][-2].content}\n"

            tool_calls = self.state["messages"][-2].tool_calls
            if tool_calls:
                last_messages_content += "Tool_calls: " + str(tool_calls) + "\n"

            return last_messages_content + "Resulted in:\n" + self.state["messages"][-1].content
        
        return last_messages_content
    
    def invoke(self, prompt=None):
        user_query = self._get_current_query()

        synthesize_prompt = self._get_prompt_template(prompt).format(query=user_query, last_output=self._get_last_messages_content())
        synthesize_prompt = self._compress_prompt_if_needed(synthesize_prompt)

        messages = [
            SystemMessage(content=synthesize_system_prompt),
            *self._get_last_messages(agent_messages_to_exclude=["query_decompose_agent_messages"]),
            HumanMessage(content=synthesize_prompt)
        ]

        llm = self.get_llm()
        ai_message = llm.invoke(messages)
        ai_message = self._add_metadata_to_message(ai_message, self.state["current_query_index"])

        synthesize_response = ai_message.content

        self.update_state("messages", [ai_message])
        self.update_state("synthesize_agent_messages", [ai_message])
        self.update_state("next_action", synthesize_response)

        return self.state