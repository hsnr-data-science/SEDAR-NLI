import os
import shutil
from langchain.schema.runnable.config import RunnableConfig
from langchain_core.messages import HumanMessage
import chainlit as cl
from chainlit.user_session import UserSession
from chainlit.input_widget import Select, Switch, Slider
from models.config import ModelConfig, Servers, Models
from states.agent_graph_state import get_initial_state, reset_state
from main import setup

if __name__ == "__main__":
    from chainlit.cli import run_chainlit

    run_chainlit(__file__)


class ChatHandler:
    WORKFLOW_SETTINGS_KEYS = [
        "server",
        "model",
        "human_confirmation",
        "prompt_compression",
        "temperature",
        "reasoning_effort",
    ]
    STATE_SETTINGS_KEYS = ["jwt", "workspaceId", "jupyterToken"]
    DEFAULT_SERVER = Servers.AZURE_OPENAI
    DEFAULT_MODEL = Models.O3_MINI

    LANGGRAPH_STEPS_TO_IGNORE = [
        "__start__",
        "ChannelWrite",
        "ChannelRead",
        "RunnableLambda",
        "_execute",
        "query_decompose_agent",
        "AzureChatOpenAI",
        "ChatOllama",
        "ChatOpenAI",
        "AzureAIChatCompletionsModel",
        "ChatGoogleGenerativeAI",
        "should_retry",
        "LangGraph",
        "sedar_agent",
        "final_response_agent",
        "ContextualCompressionRetriever",
        "VectorStoreRetriever",
        "manager_agent",
        "tool_agent",
        "code_agent",
        "synthesize_agent",
        "tools",
        "update_tool_state",
        "tool_agent",
        "create_dataset_agent",
        "create_dataset_tool",
        "ml_create_agent",
        "ml_create_tool",
        "search_datasets_agent",
        "search_datasets_tool",
        "next_action",
        "run_query",
        "should_try_again",
        "_write",
    ]

    @staticmethod
    def cleanup_files(directory="./.files"):
        if os.path.exists(directory):
            for filename in os.listdir(directory):
                file_path = os.path.join(directory, filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                except Exception as e:
                    print(f"Failed to delete {file_path}. Reason: {e}")

    @staticmethod
    def get_message_file(message: cl.Message):
        return next(
            (element for element in message.elements if isinstance(element, cl.File)),
            None,
        )

    @staticmethod
    def store_file(file, directory="./.files"):
        original_filename = file.name
        source_path = file.path
        destination_path = os.path.join(directory, original_filename)
        os.makedirs(directory, exist_ok=True)
        shutil.move(source_path, destination_path)

    @staticmethod
    def get_chainlit_settings_elements():
        servers = [
            value for key, value in vars(Servers).items() if not key.startswith("__")
        ]
        models = [
            value for key, value in vars(Models).items() if not key.startswith("__")
        ]
        reasoning_effort = ["low", "medium", "high"]

        server_initial_index = servers.index(ChatHandler.DEFAULT_SERVER)
        model_initial_index = models.index(ChatHandler.DEFAULT_MODEL)

        return [
            Select(
                id="server",
                label="Server",
                values=servers,
                initial_index=server_initial_index,
            ),
            Select(
                id="model",
                label="Model",
                values=models,
                initial_index=model_initial_index,
            ),
            Select(
                id="reasoning_effort",
                label="Reasoning Effort",
                values=reasoning_effort,
                initial_index=0,
            ),
            Slider(
                id="temperature", label="Temperature", min=0, max=2, step=0.1, initial=0
            ),
            Switch(id="human_confirmation", label="Human Confirmation", initial=False),
            Switch(id="prompt_compression", label="Prompt Compression", initial=False),
        ]

    @staticmethod
    def get_langgraph_workflow(user_session: UserSession):
        server = user_session.get("server")
        model = user_session.get("model")
        human_confirmation = user_session.get("human_confirmation")
        prompt_compression = user_session.get("prompt_compression")
        temperature = user_session.get("temperature")
        reasoning_effort = user_session.get("reasoning_effort")

        return setup(
            ModelConfig(
                server=server,
                model=model,
                temperature=temperature,
                reasoning_effort=reasoning_effort,
            ),
            human_confirmation=human_confirmation,
            prompt_compression=prompt_compression,
        )

    @staticmethod
    def is_user_authenticated(user_session: UserSession):
        return all(
            user_session.get(key) is not None for key in ChatHandler.STATE_SETTINGS_KEYS
        )

    @staticmethod
    def has_all_keys(user_session: UserSession):
        return all(
            user_session.get(key) is not None
            for key in ChatHandler.WORKFLOW_SETTINGS_KEYS
        )


@cl.on_chat_start
async def start():
    ChatHandler.cleanup_files()
    settings = await cl.ChatSettings(
        ChatHandler.get_chainlit_settings_elements()
    ).send()
    await setup_agent(settings)


@cl.on_settings_update
async def setup_agent(settings):
    for key, value in settings.items():
        if cl.user_session.get(key) != value:
            if key in ChatHandler.WORKFLOW_SETTINGS_KEYS:
                cl.user_session.set("workflow", None)
            elif key in ChatHandler.STATE_SETTINGS_KEYS:
                cl.user_session.set("state", None)

            cl.user_session.set(key, value)

    jwt, workspace_id, jupyter_token = (
        cl.user_session.get("jwt"),
        cl.user_session.get("workspaceId"),
        cl.user_session.get("jupyterToken"),
    )

    if not ChatHandler.is_user_authenticated(cl.user_session):
        raise Exception("Error: Not authenticated")

    if not ChatHandler.has_all_keys(cl.user_session):
        return  # Wait for another settings update event

    if not cl.user_session.get("workflow"):
        cl.user_session.set(
            "workflow", ChatHandler.get_langgraph_workflow(cl.user_session)
        )

    if not cl.user_session.get("state"):
        cl.user_session.set(
            "state", get_initial_state("", jwt, jupyter_token, workspace_id)
        )


@cl.on_message
async def on_message(message: cl.Message):
    if file := ChatHandler.get_message_file(message):
        ChatHandler.store_file(file)

    workflow = cl.user_session.get("workflow")
    state = cl.user_session.get("state")
    state = reset_state(state, message.content)
    config = {"configurable": {"thread_id": cl.context.session.id}}
    callback_handler = cl.LangchainCallbackHandler(
        to_ignore=ChatHandler.LANGGRAPH_STEPS_TO_IGNORE  # Filter out steps that shouldn't be shown in the UI
    )
    final_answer = cl.Message(content="")

    try:
        async for stream_type, stream_message in workflow.astream(
            state,
            stream_mode=["messages", "values"],
            config=RunnableConfig(callbacks=[callback_handler], **config),
        ):
            if stream_type == "messages":
                msg, metadata = stream_message
                if (
                    msg.content
                    and not isinstance(msg, HumanMessage)
                    and metadata["langgraph_node"] == "final_response_agent"
                ):
                    await final_answer.stream_token(msg.content)
            elif stream_type == "values":
                state = stream_message
                if state["final_response"]:
                    cl.user_session.set("state", state)
    except Exception as e:
        final_answer = cl.ErrorMessage(content=str(e))
    finally:
        await final_answer.send()
