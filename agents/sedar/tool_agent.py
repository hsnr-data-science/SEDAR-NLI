from langchain_core.tools.base import BaseTool
from langchain_core.messages import SystemMessage, HumanMessage, AIMessage
from states.consts import CONTINUE_ACTION
from prompts.prompts import tool_system_prompt, tool_prompt_template, tool_prompt_template_compressed
from cache.cacheable import CacheableRegistry
from .sedar_agent import SedarAgent

class ToolAgent(SedarAgent):
    def __init__(self, state, tool_retriever, model_config, prompt_compression: bool, source_node = "tool_agent"):
        super().__init__(state, tool_retriever, model_config, prompt_compression, source_node)
        self.prompt_template = tool_prompt_template
        self.prompt_template_compressed = tool_prompt_template_compressed

    def _add_kwargs_to_tool_call(self, ai_message: AIMessage) -> AIMessage:
        """This function is used to add missing kwargs parameters to tool calls for our custom_functions.

        We use this kwargs parameter to pass the object_cache, sedar_api, and initial_query to our custom functions.
        Some LLMs fail to generate this which causes a Pydantic error.
        """
        if ai_message.tool_calls and ai_message.tool_calls[0]["name"] in CacheableRegistry.get_all_registered_methods():
            if "kwargs" not in ai_message.tool_calls[0]["args"]:
                ai_message.tool_calls[0]["args"]["kwargs"] = {}
        return ai_message

    def _get_last_failed_output(self):
        failed_output = ""

        if len(self.state["messages"]) >= 4 and \
          CONTINUE_ACTION in self.state["messages"][-2].content and \
          self.state["messages"][-4].source_node == self.source_node:
            last_tool_output = ""
            if isinstance(self.state["tool_agent_messages"][-1].content, str):
                last_tool_output = self.state["tool_agent_messages"][-1].content

            last_tool_output += "\n" + str(self.state["tool_agent_messages"][-1].tool_calls)
            last_tool_execution_output = self.state["tool_execution_messages"][-1].content

            failed_output = f"Your last code output:\n{last_tool_output}\n\nResulted in:\n{last_tool_execution_output}\nMaybe try a different tool.\n"

        return failed_output
    
    def invoke(self, tools: list[BaseTool], prompt=None):
        query = self._get_current_query()
        tools = self.tool_retriever.get_tools(query, tools)

        tool_prompt = self._get_prompt_template(prompt).format(
            query=query,
            class_info=self._get_class_info(self.state["current_instance"]),
            object_cache=self._serialize_json_miminal(self.state["object_cache"]),
            last_output=self._get_last_failed_output()
        )
        tool_prompt = self._compress_prompt_if_needed(tool_prompt)

        past_messages_to_filter = ["query_decompose_agent_messages", "manager_agent_messages", "code_agent_messages", "code_execution_messages"]

        messages = [
            SystemMessage(content=tool_system_prompt),
            *self._get_last_messages(agent_messages_to_exclude=past_messages_to_filter),
            HumanMessage(content=tool_prompt)
        ]

        llm = self.get_llm(tools=tools)
        ai_message = llm.invoke(messages)
        ai_message = self._add_kwargs_to_tool_call(ai_message)
        ai_message = self._add_metadata_to_message(ai_message, self.state["current_query_index"])
        ai_message.tool_calls = ai_message.tool_calls[:1]  # Only allow one tool call

        self.update_state("messages", [ai_message])
        self.update_state("tool_agent_messages", [ai_message])

        return self.state
