import re
from langchain_core.messages import HumanMessage
from states.sedar_agent_state import SedarAgentState
from cache.cacheable import CacheableRegistry
from typing import Tuple, Any
from tools.object_cache_repl import ObjectCachePythonREPL

class CodeExecutionTool:
    """
    A tool for executing Python code snippets within the REPL, integrated with state management.

    Attributes:
        state (SedarAgentState): The current state of the SEDAR agent graph.
    """

    def __init__(self, state: SedarAgentState):
        self.state = state
        self.current_instance = state["current_instance"]
        self.object_cache = state["object_cache"]
        self.repl = ObjectCachePythonREPL(state)

    def _inject_kwargs(self, code: str) -> str:
        """Injects additional keyword arguments into custom function calls,
        ensuring proper syntax.
        """
        registered_methods = CacheableRegistry.get_all_registered_methods()

        for target_class, methods in registered_methods.items():
            for method_name, method_func in methods.items():
                # Find calls to the method (e.g., "label_modeling_attributes(...")
                pattern = rf"({method_name}\()([^\)]*)"  # Match method_name followed by arguments
                matches = re.finditer(pattern, code)

                for match in matches:
                    full_match = match.group(0)
                    existing_args = match.group(2)

                    # Ensure kwargs come after positional arguments
                    if existing_args.strip():
                        modified_args = f"{existing_args}, object_cache=object_cache, sedar_api=sedar_api, initial_query=initial_query"
                    else:
                        modified_args = "object_cache=object_cache, sedar_api=sedar_api, initial_query=initial_query"

                    # Replace original function call with modified one
                    new_call = f"{method_name}({modified_args}"
                    code = code.replace(full_match, new_call)

        return code

    def run_code(self, code: str) -> Tuple[str, dict]:
        """
        Executes a Python code snippet within the REPL.

        Args:
            code: The Python code snippet to run.

        Returns:
            str: The output of the code execution.
            dict: The updated object cache.
        """
        code = self._inject_kwargs(code)
        result = self.repl.run(code)
        updated_cache = self.repl.get_cache()
        return result, updated_cache

    def run(self):
        """
        Executes the last message content as code and updates the state.
        """
        last_message = self.state["messages"][-1]
        code_output, updated_cache = self.run_code(last_message.content)
        code_execution_message = HumanMessage(content=code_output)
        code_execution_message.source_node = "code_executor"
        code_execution_message.query_index = self.state["current_query_index"]

        return {
            **self.state,
            "object_cache": updated_cache,
            "messages": [code_execution_message],
            "code_execution_messages": [code_execution_message]
        }

def get_available_globals(current_instance: Any) -> str:
    """
    Provides a description of the available global variables and functions for the current instance.

    Args:
        current_instance: The current instance to work with.

    Returns:
        str: A description of the available global variables and functions.
    """
    return (
        f"""{current_instance.__class__.__name__.lower()} (instance of the current {current_instance.__class__.__name__} class you work with)\n"""
        """def get_from_cache(key): (function to retrieve objects from the cache)\n"""
        """def print(output): (function to print output, and write objects to cache (both are handled), anything that is considered final output or should be passed as information to other agents should be printed)"""
        """You are highly encouraged to use print when there is some output variable or object to store in cache to answer future queries."""
        """If you print something, or store something in cache, ALWAYS STRICTLY print the object itself, not object.content"""
    )