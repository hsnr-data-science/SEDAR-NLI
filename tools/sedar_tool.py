from langchain_core.tools import BaseTool
from langchain_core.tools.base import (
    _get_runnable_config_param,
    _handle_validation_error,
    _handle_tool_error,
    _is_message_content_type,
    _stringify,
    ToolException
)
from langchain_core.callbacks import CallbackManager, AsyncCallbackManager, Callbacks
from langchain_core.runnables import RunnableConfig, patch_config, run_in_executor
from langchain_core.runnables.config import _set_config_context
from langchain_core.runnables.utils import asyncio_accepts_context
from pydantic import BaseModel, ValidationError
from pydantic.v1 import ValidationError as ValidationErrorV1
from inspect import signature
from contextvars import copy_context
from typing import Any, Type, Union, Optional
from io import StringIO
from tools.serializer import CacheableSerializer
from tools.sedar_tool_message import SedarToolMessage
import logging
import sys
import uuid

TOOL_EXECUTION_ERROR_MSG = "An error occurred while executing the tool:"

class SedarTool(BaseTool):
    """
    Custom tool for interacting with the SEDAR API.
    """
    name: str
    description: str
    method: callable
    args_schema: Type[BaseModel]
    class_instance: Any

    # Fix to make gemini function calls more robust
    def _replace_empty_kwargs(self, tool_input: dict) -> dict:
        if "kwargs" in tool_input:
            kwargs = tool_input["kwargs"]
            if kwargs == "" or kwargs == "{}" or kwargs == "None" or kwargs == "null" or kwargs == "unknown":
                tool_input["kwargs"] = {}
        elif not "kwargs" in tool_input:
            tool_input["kwargs"] = {}
        return tool_input

    def _run(self, **kwargs) -> Any:
        result = None

        old_stdout = sys.stdout
        root_logger = logging.getLogger()
        old_handlers = root_logger.handlers[:]
        string_stdout = StringIO()
        log_stream = StringIO()
        string_log_handler = logging.StreamHandler(log_stream)
        string_log_handler.setLevel(logging.INFO)

        root_logger.handlers = [string_log_handler]
        sys.stdout = string_stdout

        try:
            if self.class_instance:
                result = self.method(self.class_instance, **kwargs)
            else:
                result = self.method(**kwargs)

        except Exception as e:
            raise ToolException(f"{TOOL_EXECUTION_ERROR_MSG} {e}")
        finally:
            string_stdout.flush()
            log_stream.flush()
            sys.stdout = old_stdout
            root_logger.handlers = old_handlers

        return result, log_stream.getvalue() + string_stdout.getvalue() + "\n" + str(CacheableSerializer.serialize_result(result))
    
    async def _arun(self, **kwargs) -> Any:
        result = None

        old_stdout = sys.stdout
        root_logger = logging.getLogger()
        old_handlers = root_logger.handlers[:]
        string_stdout = StringIO()
        log_stream = StringIO()
        string_log_handler = logging.StreamHandler(log_stream)
        string_log_handler.setLevel(logging.INFO)

        root_logger.handlers = [string_log_handler]
        sys.stdout = string_stdout

        try:
            if self.class_instance:
                result = self.method(self.class_instance, **kwargs)
            else:
                result = self.method(**kwargs)

        except Exception as e:
            raise ToolException(f"{TOOL_EXECUTION_ERROR_MSG} {e}")
        finally:
            string_stdout.flush()
            log_stream.flush()
            sys.stdout = old_stdout
            root_logger.handlers = old_handlers

        return result, log_stream.getvalue() + string_stdout.getvalue() + "\n" + str(CacheableSerializer.serialize_result(result))
    
    # This is an almost identical copy of the run method from langchain_core.tools.base.BaseTool
    # We only change the _format_output method to support our custom ToolMessage
    def run(
        self,
        tool_input: Union[str, dict[str, Any]],
        verbose: Optional[bool] = None,
        start_color: Optional[str] = "green",
        color: Optional[str] = "green",
        callbacks: Callbacks = None,
        *,
        tags: Optional[list[str]] = None,
        metadata: Optional[dict[str, Any]] = None,
        run_name: Optional[str] = None,
        run_id: Optional[uuid.UUID] = None,
        config: Optional[RunnableConfig] = None,
        tool_call_id: Optional[str] = None,
        **kwargs: Any,
    ) -> Any:
        """Run the tool.

        Args:
            tool_input: The input to the tool.
            verbose: Whether to log the tool's progress. Defaults to None.
            start_color: The color to use when starting the tool. Defaults to 'green'.
            color: The color to use when ending the tool. Defaults to 'green'.
            callbacks: Callbacks to be called during tool execution. Defaults to None.
            tags: Optional list of tags associated with the tool. Defaults to None.
            metadata: Optional metadata associated with the tool. Defaults to None.
            run_name: The name of the run. Defaults to None.
            run_id: The id of the run. Defaults to None.
            config: The configuration for the tool. Defaults to None.
            tool_call_id: The id of the tool call. Defaults to None.
            kwargs: Keyword arguments to be passed to tool callbacks

        Returns:
            The output of the tool.

        Raises:
            ToolException: If an error occurs during tool execution.
        """
        callback_manager = CallbackManager.configure(
            callbacks,
            self.callbacks,
            self.verbose or bool(verbose),
            tags,
            self.tags,
            metadata,
            self.metadata,
        )

        run_manager = callback_manager.on_tool_start(
            {"name": self.name, "description": self.description},
            tool_input if isinstance(tool_input, str) else str(tool_input),
            color=start_color,
            name=run_name,
            run_id=run_id,
            # Inputs by definition should always be dicts.
            # For now, it's unclear whether this assumption is ever violated,
            # but if it is we will send a `None` value to the callback instead
            # TODO: will need to address issue via a patch.
            inputs=tool_input if isinstance(tool_input, dict) else None,
            **kwargs,
        )

        content = None
        artifact = None
        status = "success"
        error_to_raise: Union[Exception, KeyboardInterrupt, None] = None
        try:
            child_config = patch_config(config, callbacks=run_manager.get_child())
            context = copy_context()
            context.run(_set_config_context, child_config)
            tool_input = self._replace_empty_kwargs(tool_input)
            tool_args, tool_kwargs = self._to_args_and_kwargs(tool_input, tool_call_id)
            if signature(self._run).parameters.get("run_manager"):
                tool_kwargs = tool_kwargs | {"run_manager": run_manager}
            if config_param := _get_runnable_config_param(self._run):
                tool_kwargs = tool_kwargs | {config_param: config}
            response = context.run(self._run, *tool_args, **tool_kwargs)
            if self.response_format == "content_and_artifact":
                if not isinstance(response, tuple) or len(response) != 2:
                    msg = (
                        "Since response_format='content_and_artifact' "
                        "a two-tuple of the message content and raw tool output is "
                        f"expected. Instead generated response of type: "
                        f"{type(response)}."
                    )
                    error_to_raise = ValueError(msg)
                else:
                    content, artifact = response
            else:
                content = response
        except (ValidationError, ValidationErrorV1) as e:
            if not self.handle_validation_error:
                error_to_raise = e
            else:
                content = _handle_validation_error(e, flag=self.handle_validation_error)
                status = "error"
        except ToolException as e:
            if not self.handle_tool_error:
                error_to_raise = e
            else:
                content = _handle_tool_error(e, flag=self.handle_tool_error)
                status = "error"
        except (Exception, KeyboardInterrupt) as e:
            error_to_raise = e

        if error_to_raise:
            run_manager.on_tool_error(error_to_raise)
            raise error_to_raise
        output = _format_output(content, artifact, tool_call_id, self.name, status)
        run_manager.on_tool_end(output, color=color, name=self.name, **kwargs)
        return output
    
    # This is an almost identical copy of the arun method from langchain_core.tools.base.BaseTool
    # We only change the _format_output method to support our custom ToolMessage
    async def arun(
        self,
        tool_input: Union[str, dict],
        verbose: Optional[bool] = None,
        start_color: Optional[str] = "green",
        color: Optional[str] = "green",
        callbacks: Callbacks = None,
        *,
        tags: Optional[list[str]] = None,
        metadata: Optional[dict[str, Any]] = None,
        run_name: Optional[str] = None,
        run_id: Optional[uuid.UUID] = None,
        config: Optional[RunnableConfig] = None,
        tool_call_id: Optional[str] = None,
        **kwargs: Any,
    ) -> Any:
        """Run the tool asynchronously.

        Args:
            tool_input: The input to the tool.
            verbose: Whether to log the tool's progress. Defaults to None.
            start_color: The color to use when starting the tool. Defaults to 'green'.
            color: The color to use when ending the tool. Defaults to 'green'.
            callbacks: Callbacks to be called during tool execution. Defaults to None.
            tags: Optional list of tags associated with the tool. Defaults to None.
            metadata: Optional metadata associated with the tool. Defaults to None.
            run_name: The name of the run. Defaults to None.
            run_id: The id of the run. Defaults to None.
            config: The configuration for the tool. Defaults to None.
            tool_call_id: The id of the tool call. Defaults to None.
            kwargs: Keyword arguments to be passed to tool callbacks

        Returns:
            The output of the tool.

        Raises:
            ToolException: If an error occurs during tool execution.
        """
        callback_manager = AsyncCallbackManager.configure(
            callbacks,
            self.callbacks,
            self.verbose or bool(verbose),
            tags,
            self.tags,
            metadata,
            self.metadata,
        )
        run_manager = await callback_manager.on_tool_start(
            {"name": self.name, "description": self.description},
            tool_input if isinstance(tool_input, str) else str(tool_input),
            color=start_color,
            name=run_name,
            run_id=run_id,
            # Inputs by definition should always be dicts.
            # For now, it's unclear whether this assumption is ever violated,
            # but if it is we will send a `None` value to the callback instead
            # TODO: will need to address issue via a patch.
            inputs=tool_input if isinstance(tool_input, dict) else None,
            **kwargs,
        )
        content = None
        artifact = None
        status = "success"
        error_to_raise: Optional[Union[Exception, KeyboardInterrupt]] = None
        try:
            tool_input = self._replace_empty_kwargs(tool_input)
            tool_args, tool_kwargs = self._to_args_and_kwargs(tool_input, tool_call_id)
            child_config = patch_config(config, callbacks=run_manager.get_child())
            context = copy_context()
            context.run(_set_config_context, child_config)
            func_to_check = (
                self._run if self.__class__._arun is BaseTool._arun else self._arun
            )
            if signature(func_to_check).parameters.get("run_manager"):
                tool_kwargs["run_manager"] = run_manager
            if config_param := _get_runnable_config_param(func_to_check):
                tool_kwargs[config_param] = config

            coro = context.run(self._arun, *tool_args, **tool_kwargs)
            if asyncio_accepts_context():
                response = await asyncio.create_task(coro, context=context)  # type: ignore
            else:
                response = await coro
            if self.response_format == "content_and_artifact":
                if not isinstance(response, tuple) or len(response) != 2:
                    msg = (
                        "Since response_format='content_and_artifact' "
                        "a two-tuple of the message content and raw tool output is "
                        f"expected. Instead generated response of type: "
                        f"{type(response)}."
                    )
                    error_to_raise = ValueError(msg)
                else:
                    content, artifact = response
            else:
                content = response
        except ValidationError as e:
            if not self.handle_validation_error:
                error_to_raise = e
            else:
                content = _handle_validation_error(e, flag=self.handle_validation_error)
                status = "error"
        except ToolException as e:
            if not self.handle_tool_error:
                error_to_raise = e
            else:
                content = _handle_tool_error(e, flag=self.handle_tool_error)
                status = "error"
        except (Exception, KeyboardInterrupt) as e:
            error_to_raise = e

        if error_to_raise:
            await run_manager.on_tool_error(error_to_raise)
            raise error_to_raise

        output = _format_output(content, artifact, tool_call_id, self.name, status)
        await run_manager.on_tool_end(output, color=color, name=self.name, **kwargs)
        return output

def _format_output(
    content: Any, artifact: Any, tool_call_id: Optional[str], name: str, status: str
) -> Union[SedarToolMessage, Any]:
    try:
        raw, serialized = content
    except Exception:
        raw = None
        serialized = content
    if tool_call_id:
        if not _is_message_content_type(serialized):
            serialized = _stringify(serialized)
        tool_message = SedarToolMessage(
            content=serialized,
            raw_content=raw,
            source_node = "sedar_tool",
            has_error=serialized.startswith(TOOL_EXECUTION_ERROR_MSG),
            artifact=artifact,
            tool_call_id=tool_call_id,
            name=name,
            status=status
        )
        return tool_message
    else:
        return serialized