from langchain_core.messages import SystemMessage, HumanMessage
from prompts.prompts import query_decompose_system_prompt, query_decompose_prompt_template, query_decompose_prompt_template_compressed
from .main_agent import MainAgent

class QueryDecomposeAgent(MainAgent):

    def __init__(self, state, tool_retriever, model_config, prompt_compression: bool, source_node: str = "query_decompose_agent"):
        super().__init__(state, tool_retriever, model_config, prompt_compression, source_node)
        self.prompt_template = query_decompose_prompt_template
        self.prompt_template_compressed = query_decompose_prompt_template_compressed

    def invoke(self, prompt=None):
        user_query = self.state["user_query"]
        available_classes_and_methods = self.tool_retriever.get_class_and_method_descriptions(
            user_query,
            k=7,
            include_remaining_methods=True,
            describe_all_classes=True,
            compress_prompt=self.prompt_compression
        )

        query_decompose_prompt = self._get_prompt_template(prompt).format(user_query=user_query, available_classes_and_methods=available_classes_and_methods)
        query_decompose_prompt = self._compress_prompt_if_needed(query_decompose_prompt)

        messages = [
            SystemMessage(content=query_decompose_system_prompt),
            *self._get_last_messages(),
            HumanMessage(content=query_decompose_prompt)
        ]

        llm = self.get_llm()
        ai_message = llm.invoke(messages)
        ai_message = self._add_metadata_to_message(ai_message)

        decomposed_queries = self._parse_llm_response(ai_message.content)

        print(f"Decomposed queries:\n {decomposed_queries}")

        self.update_state("messages", [ai_message])
        self.update_state("query_decompose_agent_messages", [ai_message])
        self.update_state("decomposed_queries", decomposed_queries)

        return self.state