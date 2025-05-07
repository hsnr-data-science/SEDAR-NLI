import inspect
import re
from pydantic import BaseModel, create_model, Field
from inspect import signature, Parameter
from typing import get_type_hints, Any, Type, Optional, Annotated, Union, get_origin, get_args
from functools import wraps
from langgraph.prebuilt import InjectedState
from langchain_core.messages import ToolMessage
from sedarapi import SedarAPI
from cache.cacheable import CacheableRegistry
from tools.sedar_tool import SedarTool
from tools.sedar_tool_message import SedarToolMessage
from states.sedar_agent_state import SedarAgentState
from states.consts import LAST_OUTPUT
from utils.utils import get_minimal_docstring, generate_short_uuid

class MethodSchemaGenerator:
    """
    Generates Pydantic schemas dynamically for method arguments.
    """

    @staticmethod
    def _is_optional_cacheable(field_type) -> bool:
        """
        Check if a field type is an optional cacheable type.
        """
        origin = get_origin(field_type)
        args = get_args(field_type)
        if origin is Union and len(args) == 2 and type(None) in args:
            field_type = next(arg for arg in args if arg is not type(None))
            return CacheableRegistry.is_cacheable(field_type)
        return False

    @staticmethod
    def generate(method: callable, inner_classes: Optional[dict[str, Any]] = None) -> Type[BaseModel]:
        """
        Generate a Pydantic model for a method's arguments.
        """
        inner_classes = inner_classes or {}
        sig = inspect.signature(method)
        type_hints = get_type_hints(method, localns=inner_classes)
        fields = {
            "object_cache": (Annotated[dict[str, Any], InjectedState("object_cache")], Field(..., description="Object cache to access objects in state")),
            "initial_query": (Annotated[str, InjectedState("current_query")], Field("", description="Current query string")),
            "sedar_api": (Annotated[SedarAPI, InjectedState("sedar_api")], Field(..., description="SedarAPI instance"))
        }

        def is_container_of_cacheables(typ) -> bool:
            origin = get_origin(typ)
            args = get_args(typ)

            # Check for list or dict containing cacheable types
            if origin in (list, dict) and args:
                # For lists, check the single argument
                if origin is list and CacheableRegistry.is_cacheable(args[0]):
                    return True
                # For dicts, check the value type
                if origin is dict and len(args) == 2 and CacheableRegistry.is_cacheable(args[1]):
                    return True
            return False

        for name, param in sig.parameters.items():
            if name in ('self', 'cls'):  # Skip 'self' and 'cls'
                continue

            # Get type hint or default to Any
            field_type = type_hints.get(name, Any)

            if CacheableRegistry.is_cacheable(field_type):
                fields[name] = (
                    str,
                    Field(
                        description=f"Pass {field_type.__name__} instance from state to this method using the ID by setting this parameter: _{field_type.__name__.upper()}_<ID>, e.g. _{field_type.__name__.upper()}_123"
                    )
                )
            elif MethodSchemaGenerator._is_optional_cacheable(field_type):
                origin = get_origin(field_type)
                args = get_args(field_type)
                field_type = next(arg for arg in args if arg is not type(None))
                fields[name] = (
                    Union[str, None],
                    Field(
                        None,
                        description=f"Pass this optional {field_type.__name__} instance from state to this method using the ID by setting this parameter: _{field_type.__name__.upper()}_<ID> or None if not needed, e.g. None or '_{field_type.__name__.upper()}_123'"
                    )
                )
            elif is_container_of_cacheables(field_type):
                origin = get_origin(field_type)
                args = get_args(field_type)
                if origin is list:
                    fields[name] = (
                        list[str],
                        Field(
                            description=f"Pass a list of cacheable instances from state as IDs, e.g., ['_{args[0].__name__.upper()}_123']."
                        )
                    )
                elif origin is dict:
                    key_type = args[0].__name__ if args and len(args) > 1 else "Any"
                    value_type = args[1].__name__.upper() if args and len(args) > 1 else "Any"
                    fields[name] = (
                        dict[args[0], str],
                        Field(
                            description=f"Pass a dictionary with keys of type {key_type} and values as IDs for cacheable instances, e.g., {{'key': '_{value_type}_123'}}."
                        )
                    )
            else:
                # Add default value if present
                if param.default != inspect.Parameter.empty:
                    fields[name] = (field_type, param.default)
                else:
                    fields[name] = (field_type, ...)

        class Config:
            arbitrary_types_allowed = True

        model_name = f"{method.__name__.capitalize()}Args"
        return create_model(model_name, **fields, __config__=Config)


class ToolGenerator:
    """
    A generic tool generator for any class.
    """

    def __init__(self, class_instance: Any, full_doc_strings: bool = True):
        self.target_class = class_instance.__class__
        self.class_instance = class_instance
        self.inner_classes = self._get_inner_classes()
        self.full_doc_strings = full_doc_strings

    def _get_inner_classes(self) -> dict[str, Any]:
        """
        Get inner classes of the target class.
        """
        return {
            name: obj
            for name, obj in inspect.getmembers(self.target_class, predicate=inspect.isclass)
            if obj.__module__ == self.target_class.__module__
        }

    @staticmethod
    def _replace_object_cache_references(kwargs: dict[str, Any], object_cache: dict[str, Any]) -> None:
        """
        Replaces any string references to objects in `kwargs` that match the pattern
        _FIELD_TYPE_<ID> with corresponding objects from the `object_cache`.
        Handles nested structures like lists and dicts.
        """
        pattern = re.compile(r'^_(\w+)_(\w+)$')
        
        def replace_value(value):
            """
            Recursively replace string references with objects from the object cache.
            """
            if value is None:
                return None
            elif isinstance(value, str):
                match = pattern.match(value)
                if match and value in object_cache:
                    return object_cache[value]
            elif isinstance(value, list):
                # Recursively process lists
                return [replace_value(item) for item in value]
            elif isinstance(value, dict):
                # Recursively process dictionaries
                return {key: replace_value(val) for key, val in value.items()}
            return value

        # Process top-level keys in kwargs
        for key, value in list(kwargs.items()):
            kwargs[key] = replace_value(value)
    
    @staticmethod
    def create_method_wrapper(method: callable) -> callable:
        """
        Create a wrapper for a method that removes the 'object_cache' parameter if present,
        and uses it to populate other kwargs that match the pattern _{field_type.__name__.upper()}_<ID>.
        """
        @wraps(method)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            object_cache = kwargs.pop('object_cache', None)
            initial_query = kwargs.pop('initial_query', None)
            sedar_api = kwargs.pop('sedar_api', None)
            sig = signature(method)
            
            # Check if the method accepts **kwargs
            has_kwargs = any(
                param.kind == Parameter.VAR_KEYWORD
                for param in sig.parameters.values()
            )
            
            # Then we explicitly pass the initial_query, object_cache, sedar_api (used for custom_functions.py)
            if has_kwargs:
                kwargs['object_cache'] = object_cache
                kwargs['initial_query'] = initial_query
                kwargs['sedar_api'] = sedar_api

            ToolGenerator._replace_object_cache_references(kwargs, object_cache)
            return method(*args, **kwargs)

        return wrapper

    def generate_tools(self) -> list[SedarTool]:
        """
        Create tools for all public methods of the target class.
        """
        tools = []

        for _, method in CacheableRegistry.get_methods(self.target_class):
            if CacheableRegistry.should_use_method(method):
                args_schema = MethodSchemaGenerator.generate(method, self.inner_classes)
                wrapped_method = self.create_method_wrapper(method)
                # Limit the docstring to 1024 characters to support OpenAI models
                doc_string = method.__doc__ if self.full_doc_strings else get_minimal_docstring(method.__doc__, ["Description", "Notes", "Raises"])[:1024]

                tools.append(
                    SedarTool(
                        name=method.__name__,
                        description=doc_string or "No description provided.",
                        method=wrapped_method,
                        args_schema=args_schema,
                        class_instance=self.class_instance,
                        handle_tool_error=True
                    )
                )
        return tools


class ToolManager:
    """
    High-level manager for generating tools for any target class.
    """

    @staticmethod
    def get_tools(class_instance: Any, full_doc_strings: bool = True) -> list[SedarTool]:
        """
        Retrieve all tools for a given class and instance.
        """
        generator = ToolGenerator(class_instance, full_doc_strings)
        return generator.generate_tools()

def store_raw_output_in_cache(object_cache: dict[str, Any], raw_output: Any) -> None:
    """Stores cacheable raw output in the object cache. Up to 3 outputs are stored if the output is a list."""
    outputs = raw_output if isinstance(raw_output, list) and len(raw_output) <= 3 else [raw_output]

    for value in outputs:
        if CacheableRegistry.is_cacheable(value):
            object_name = value.__class__.__name__.upper()
            object_cache[f"_{object_name}_{generate_short_uuid()}"] = value

def unpack_single_element_list(value: Any) -> Any:
    """Unpacks a single-element list."""
    return value[0] if isinstance(value, list) and len(value) == 1 else value

def update_tool_state(state: SedarAgentState) -> SedarAgentState:
    """Updates the state with the latest tool message's raw output."""
    messages = state.get("messages", [])
    object_cache = state.get("object_cache", {})
    
    if not messages:
        return state

    last_message = messages[-1]
    if isinstance(last_message, ToolMessage) or isinstance(last_message, SedarToolMessage):
        last_message.source_node = "sedar_tool"
    state = {**state, "tool_execution_messages": [last_message]}

    if isinstance(last_message, SedarToolMessage):
        raw_output = unpack_single_element_list(last_message.raw_output)
        if raw_output is not None:
            object_cache[LAST_OUTPUT] = raw_output

        for msg in reversed(messages):
            if isinstance(msg, SedarToolMessage):
                store_raw_output_in_cache(object_cache, msg.raw_output)
            else:
                break

        state["object_cache"] = object_cache

    return state
