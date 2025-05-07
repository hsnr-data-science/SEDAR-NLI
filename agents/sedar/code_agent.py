from langchain_core.messages import SystemMessage, HumanMessage
from prompts.prompts import code_system_prompt, code_prompt_template, code_prompt_template_compressed
from tools.code_tool import get_available_globals
from states.consts import CONTINUE_ACTION
from .sedar_agent import SedarAgent

class CodeAgent(SedarAgent):

    def __init__(self, state, tool_retriever, model_config, prompt_compression: bool, source_node = "code_agent"):
        super().__init__(state, tool_retriever, model_config, prompt_compression, source_node)
        self.prompt_template = code_prompt_template
        self.prompt_template_compressed = code_prompt_template_compressed

    def _get_last_failed_output(self):
        failed_output = ""

        if len(self.state["messages"]) >= 4 and \
          CONTINUE_ACTION in self.state["messages"][-2].content and \
          self.state["messages"][-4].source_node == "code_agent":
            last_code_output = self.state["code_agent_messages"][-1].content
            last_code_execution_output = self.state["code_execution_messages"][-1].content

            failed_output = f"Your last code output:\n{last_code_output}\n\nResulted in:\n{last_code_execution_output}\n"

        return failed_output
    
    def _format_as_python_code(self, code: str) -> str:
        if not code.startswith("```python"):
            code = "```python\n" + code
        if not code.endswith("```"):
            code += "```"
        return code
    
    def invoke(self, prompt=None):
        current_query = self._get_current_query()
        available_classes_and_methods = self.tool_retriever.get_class_and_method_descriptions(current_query, k=7, compress_prompt=self.prompt_compression)
        globals = get_available_globals(self.state["current_instance"])
        object_cache = self._serialize_json_miminal(self.state["object_cache"])

        code_prompt = self._get_prompt_template(prompt).format(
            query=current_query,
            available_classes_and_methods=available_classes_and_methods,
            globals=globals,
            object_cache=object_cache,
            last_output=self._get_last_failed_output()
        )
        code_prompt = self._compress_prompt_if_needed(code_prompt)

        past_messages_to_filter = ["query_decompose_agent_messages", "manager_agent_messages", "tool_agent_messages"]

        messages = [
            SystemMessage(content=code_system_prompt),
            *self._get_last_messages(agent_messages_to_exclude=past_messages_to_filter),
            HumanMessage(content=code_prompt)
        ]

        llm = self.get_llm()
        ai_message = llm.invoke(messages)
        ai_message.content = self._format_as_python_code(ai_message.content)
        ai_message = self._add_metadata_to_message(ai_message, self.state["current_query_index"])

        self.update_state("messages", [ai_message])
        self.update_state("code_agent_messages", [ai_message])

        return self.state