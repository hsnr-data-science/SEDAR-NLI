from langchain_experimental.utilities import PythonREPL
from cache.cacheable import CacheableRegistry
from utils.utils import generate_short_uuid
from utils.custom_json_encoder import ExtendedEncoder
from states.sedar_agent_state import SedarAgentState
from states import LAST_OUTPUT
from typing import Any, Dict, Optional
from io import StringIO
import multiprocessing
import logging
import json
import sys


class ObjectCachePythonREPL(PythonREPL):
    """
    A REPL extension with support for object caching and custom globals.
    """

    def __init__(self, state: SedarAgentState):
        super().__init__()
        object_cache = state["object_cache"]
        current_instance = state["current_instance"]
        sedar_api = state["sedar_api"]
        initial_query = state["current_query"]
        self.globals = {
            current_instance.__class__.__name__.lower(): current_instance,
            "object_cache": object_cache,
            "get_from_cache": self.get_from_cache,
            "print": self.pout,
            "sedar_api": sedar_api,
            "initial_query": initial_query
        }
        self.locals = {}

    def get_from_cache(self, key):
        return self.globals["object_cache"].get(key)

    def pout(self, output):
        """Override the print function to write the output to the object cache."""
        output = self._unpack_single_element_list(output)

        self.globals["object_cache"][LAST_OUTPUT] = output

        if CacheableRegistry.is_cacheable(output):
            key = f"_{output.__class__.__name__.upper()}_{generate_short_uuid()}"
            self.globals["object_cache"][key] = output
            if hasattr(output, "content"):
                output = output.content

        output = json.dumps(output, cls=ExtendedEncoder)
        print(output)

    def get_cache(self):
        return self.globals["object_cache"]
    
    def _unpack_single_element_list(self, value):
        if isinstance(value, list) and len(value) == 1:
            return value[0]
        return value
    
    @classmethod
    def worker(
        cls,
        command: str,
        globals: Optional[Dict],
        locals: Optional[Dict],
        queue: multiprocessing.Queue,
    ) -> None:
        old_stdout = sys.stdout 
        mystdout = StringIO()
        log_stream = StringIO()
        log_handler = logging.StreamHandler(log_stream)

        root_logger = logging.getLogger()
        old_handlers = root_logger.handlers[:]
        root_logger.handlers = [log_handler]
        log_handler.setLevel(logging.INFO)

        sys.stdout = mystdout
        try:
            cleaned_command = cls.sanitize_input(command)
            exec(cleaned_command, globals, locals)
            mystdout.flush()
            log_handler.flush()

            queue.put(log_stream.getvalue() + mystdout.getvalue())
        except Exception as e:
            queue.put(repr(e))
        finally:
            sys.stdout = old_stdout
            root_logger.handlers = old_handlers
