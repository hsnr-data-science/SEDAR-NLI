import inspect
from langchain_core.messages import SystemMessage, HumanMessage
from prompts.prompts import manager_system_prompt, manager_prompt_template, manager_prompt_template_compressed
from states.sedar_agent_state import get_remaining_objects, get_tool_objects
from states.consts import TOOL_ACTION, CODE_ACTION, CONTINUE_ACTION, ERROR_ACTION, DECLINE_ACTION
from cache.cacheable import CacheableRegistry
from .sedar_agent import SedarAgent

class ManagerAgent(SedarAgent):

    def __init__(self, state, tool_retriever, model_config, prompt_compression: bool, source_node = "manager_agent"):
        super().__init__(state, tool_retriever, model_config, prompt_compression, source_node)
        self.prompt_template = manager_prompt_template
        self.prompt_template_compressed = manager_prompt_template_compressed

    def _get_class_methods(self, current_instance):
        signatures = []
        for _, method in CacheableRegistry.get_methods(current_instance.__class__):
            if CacheableRegistry.should_use_method(method):
                signatures.append(f"def {method.__name__}{str(inspect.signature(method))}")

        return "\n".join(signatures)

    def _get_last_action(self):
        if len(self.state["messages"]) < 3:
            return "No previous action."
        
        last_action = f"{self.state['next_action']}\n"
        
        first_message = self.state["messages"][-2]
        
        if hasattr(first_message, "tool_calls") and first_message.tool_calls:
            last_action += f"Tool calls: {first_message.tool_calls}\n"
        else:
            last_action += f"{first_message.content}\n"

        return last_action + self.state["messages"][-1].content
    
    def _get_and_update_current_query(self):
        self.update_state("current_query_number_of_tries", self.state["current_query_number_of_tries"] + 1)
        if CONTINUE_ACTION not in self.state["next_action"] and ERROR_ACTION not in self.state["next_action"] and DECLINE_ACTION not in self.state["next_action"]:
            self.update_state("current_query_number_of_tries", 0)
            current_query_index = self.state["current_query_index"] + 1
            self.update_state("current_query_index", current_query_index)
            current_query = self._get_current_query()
            self.update_state("current_query", current_query)
            return current_query
        return self._get_current_query()
    
    def _get_last_failed_output(self):
        failed_output = ""

        if ERROR_ACTION in self.state["next_action"]:
            last_output = self.state["manager_agent_messages"][-1].content
            failed_output = f"Your last failed output:\n{last_output}\nTry again.\n"

        elif CONTINUE_ACTION in self.state["next_action"]:
            last_output = self.state["manager_agent_messages"][-1].content
            failed_output = f"Your last output:\n{last_output}\n\nResulted in:\n"

            if TOOL_ACTION in last_output:
                if isinstance(self.state["tool_agent_messages"][-1].content, str):
                    failed_output += self.state["tool_agent_messages"][-1].content
                if self.state["tool_execution_messages"]:
                    failed_output += f"\n{self.state['tool_execution_messages'][-1].content}"

            elif CODE_ACTION in last_output:
                failed_output += self.state["code_agent_messages"][-1].content
                if self.state["code_execution_messages"]:
                    failed_output += f"\n{self.state['code_execution_messages'][-1].content}"

            failed_output += "\n\nMaybe try something different.\n"    
            
        return failed_output
    
    def invoke(self, prompt=None):
        current_query = self._get_and_update_current_query()

        tool_objects = self._serialize_json_miminal(get_tool_objects(self.state))
        remaining_cache_objects = self._serialize_json_miminal(get_remaining_objects(self.state))
        available_classes_and_methods = self.tool_retriever.get_class_and_method_descriptions(current_query, k=7, compress_prompt=self.prompt_compression)

        manager_prompt = self._get_prompt_template(prompt).format(
            user_query=current_query,
            tool_objects=tool_objects,
            cache_objects=remaining_cache_objects,
            available_classes_and_methods=available_classes_and_methods,
            last_output=self._get_last_failed_output()
        )
        manager_prompt = self._compress_prompt_if_needed(manager_prompt)

        messages = [
            SystemMessage(content=manager_system_prompt),
            *self._get_last_messages(agent_messages_to_exclude=["query_decompose_agent_messages"]),
            HumanMessage(content=manager_prompt)
        ]

        llm = self.get_llm()
        ai_message = llm.invoke(messages)
        ai_message = self._add_metadata_to_message(ai_message, self.state["current_query_index"])

        try:
            manager_response = self._parse_llm_response(ai_message.content)
            action = manager_response["action"]
            tool_object = manager_response["tool_object"]

            if TOOL_ACTION in action:
                self.update_state("current_instance", self.state["object_cache"][tool_object])

            print(action)
        
        except Exception as e:
            ai_message.content = ai_message.content + f"\n\nThis gave the following error: {repr(e)}"
            action = ERROR_ACTION

        finally:
            return {
                **self.state,
                "messages": [ai_message],
                "manager_agent_messages": [ai_message],
                "next_action": action
            }