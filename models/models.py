from langchain_ollama import ChatOllama, OllamaEmbeddings
from langchain_openai import ChatOpenAI, OpenAIEmbeddings, AzureChatOpenAI, AzureOpenAIEmbeddings
from langchain_azure_ai.chat_models import AzureAIChatCompletionsModel
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_anthropic import ChatAnthropic
from langchain_cohere import ChatCohere

import os
from models import config

def is_reasoning_model(model):
    """Returns whether the model is a reasoning model."""
    return model in [config.Models.O3_MINI, config.Models.O1, config.Models.O4_MINI]

def create_ollama(model_config: config.ModelConfig, base_url: str, tools=[]):
    """Creates an Ollama LLM with optional tool binding."""
    llm = ChatOllama(
        model=model_config.model,
        base_url=base_url,
        temperature=model_config.temperature,
        client_kwargs={"headers": {"Authorization": f"Bearer {os.getenv('CORINTH_RWTH_API_KEY')}"}},
        rate_limiter=model_config.rate_limiter
    )
    return llm.bind_tools(tools, tool_choice="any") if tools else llm

def create_openai(model_config: config.ModelConfig, tools=[]):
    """Creates an OpenAI LLM with optional tool binding."""
    llm = ChatOpenAI(
        model=model_config.model,
        api_key=os.getenv("OPENAI_API_KEY"),
        temperature=model_config.temperature,
        rate_limiter=model_config.rate_limiter
    )
    return llm.bind_tools(tools, tool_choice="any") if tools else llm

def create_azure_openai(model_config: config.ModelConfig, tools=[]):
    """Creates an Azure OpenAI LLM with optional tool binding."""
    llm = AzureChatOpenAI(
        model=model_config.model,
        azure_endpoint=os.getenv("AZURE_OPENAI_URL"),
        api_key=os.getenv("AZURE_OPENAI_API_KEY"),
        api_version="2024-12-01-preview",
        temperature=None if is_reasoning_model(model_config.model) else model_config.temperature,
        reasoning_effort=model_config.reasoning_effort if is_reasoning_model(model_config.model) else None,
        rate_limiter=model_config.rate_limiter
    )
    return llm.bind_tools(tools, tool_choice="required") if tools else llm

def create_azure_ml(model_config: config.ModelConfig, tools=[]):
    """Creates an Azure ML LLM with optional tool binding."""
    llm = AzureAIChatCompletionsModel(
        endpoint=os.getenv("AZURE_ML_LLAMA3_1_URL"),
        credential=os.getenv("AZURE_ML_LLAMA3_1_KEY"),
        temperature=model_config.temperature,
        rate_limiter=model_config.rate_limiter,
        max_tokens=4096
    )
    return llm.bind_tools(tools, tool_choice="any") if tools else llm

def create_google_generative_ai(model_config: config.ModelConfig, tools=[]):
    """Creates a Google Generative AI LLM with optional tool binding."""
    llm = ChatGoogleGenerativeAI(
        model=model_config.model,
        temperature=model_config.temperature,
        max_tokens=None,
        max_retries=3,
        api_key=os.getenv("GOOGLE_API_KEY"),
        rate_limiter=model_config.rate_limiter
    )
    return llm.bind_tools(tools, tool_choice="any") if tools else llm

def create_anthropic(model_config: config.ModelConfig, tools=[]):
    """Creates an Anthropic LLM with optional tool binding."""
    llm = ChatAnthropic(
        model=model_config.model,
        temperature=model_config.temperature,
        max_tokens=4096,
        max_retries=7,
        api_key=os.getenv("ANTHROPIC_API_KEY"),
        rate_limiter=model_config.rate_limiter
    )
    return llm.bind_tools(tools, tool_choice="any") if tools else llm

def create_cohere(model_config: config.ModelConfig, tools=[]):
    """Creates a Cohere LLM with optional tool binding."""
    llm = ChatCohere(
        model=model_config.model,
        temperature=model_config.temperature,
        max_tokens=4096,
        max_retries=3,
        api_key=os.getenv("COHERE_API_KEY"),
        rate_limiter=model_config.rate_limiter
    )
    return llm.bind_tools(tools) if tools else llm

def create_embeddings(model, base_url=None, api_key=None, client_kwargs=None):
    """Creates embeddings for Ollama or OpenAI."""
    if base_url:
        return OllamaEmbeddings(model=model, base_url=base_url, client_kwargs=client_kwargs)
    return OpenAIEmbeddings(model=model, api_key=api_key)

def get_model(model_config: config.ModelConfig, tools=[]):
    """Returns an appropriate model or embedding based on the configuration."""
    # Embeddings
    if model_config.embedding_size is not None:
        if model_config.server == config.Servers.OLLAMA_RWTH:
            return OllamaEmbeddings(
                model=model_config.model,
                base_url=os.getenv("CORINTH_RWTH_URL"),
                client_kwargs={"headers": {"Authorization": f"Bearer {os.getenv('CORINTH_RWTH_API_KEY')}"}}
            )
        elif model_config.server == config.Servers.OLLAMA_HSNR:
            return OllamaEmbeddings(
                model=model_config.model,
                base_url=os.getenv("OPENWEBUI_HSNR_URL")
            )
        elif model_config.server == config.Servers.AZURE_OPENAI:
            return AzureOpenAIEmbeddings(
                model=model_config.model,
                api_key=os.getenv("AZURE_OPENAI_API_KEY"),
                azure_endpoint=os.getenv("AZURE_OPENAI_URL"),
                api_version="2024-12-01-preview",
                dimensions=model_config.embedding_size
            )
        elif model_config.server == config.Servers.OPENAI:
            return OpenAIEmbeddings(
                model=model_config.model,
                api_key=os.getenv("OPENAI_API_KEY"),
                dimensions=model_config.embedding_size
            )

    # LLMs
    if model_config.server == config.Servers.OLLAMA_RWTH:
        return create_ollama(model_config, base_url=os.getenv("CORINTH_RWTH_URL"), tools=tools)
    elif model_config.server == config.Servers.OLLAMA_HSNR:
        return create_ollama(model_config, base_url=os.getenv("OPENWEBUI_HSNR_URL"), tools=tools)
    elif model_config.server == config.Servers.OPENAI:
        return create_openai(model_config, tools=tools)
    elif model_config.server == config.Servers.AZURE_OPENAI:
        return create_azure_openai(model_config, tools=tools)
    elif model_config.server == config.Servers.AZURE_ML:
        return create_azure_ml(model_config, tools=tools)
    elif model_config.server == config.Servers.GOOGLE:
        return create_google_generative_ai(model_config, tools=tools)
    elif model_config.server == config.Servers.ANTHROPIC:
        return create_anthropic(model_config, tools=tools)
    elif model_config.server == config.Servers.COHERE:
        return create_cohere(model_config, tools=tools)

    raise ValueError("Invalid server configuration.")
