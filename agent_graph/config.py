from pydantic import BaseModel
from typing import Any
from models.config import ModelConfig

class BaseGraphConfig(BaseModel):
    default_llm: ModelConfig
    embedding_model: ModelConfig
    full_doc_strings: bool
    prompt_compression: bool
    human_confirmation: bool

class MainGraphConfig(BaseGraphConfig):
    pass
