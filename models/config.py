from pydantic import BaseModel, Field
from typing import Optional
from langchain_core.rate_limiters import BaseRateLimiter

# These seem to work decently:
# hermes3:70b
# llama3.1:70b
# qwen2.5:72b
# qwen2:72b-instruct

class ModelConfig(BaseModel):
    server: str = Field(..., description="The server hosting the model")
    model: str = Field(..., description="The model identifier or name")
    embedding_size: Optional[int] = Field(None, description="The size of the embeddings")
    temperature: Optional[float] = Field(0, description="The temperature for the model")
    reasoning_effort: Optional[str] = Field("low", description="The reasoning effort for the model (low, medium, high) only for o1 or o3 models")
    rate_limiter: Optional[BaseRateLimiter] = Field(None, description="Rate limiter for the model")

    class Config:
        arbitrary_types_allowed = True
        frozen = True

class Servers:
    OLLAMA_RWTH = "ollama_rwth"
    OLLAMA_HSNR = "ollama_hsnr"
    OPENAI = "openai"
    AZURE_OPENAI = "azure_openai"
    AZURE_ML = "azure_ml"
    GOOGLE = "google"
    ANTHROPIC = "anthropic"
    COHERE = "cohere"

class Models:
    FINETUNED_LLAMA3_3 = "finetunedllama:latest"
    FINETUNED_QWEN2_5 = "finetuned-qwen:latest"
    LLAMA3_1 = "llama3.1:70b"
    LLAMA3_1_405B = "llama3.1:405b"
    LLAMA3_3 = "llama3.3:latest"
    QWEN2_5 = "qwen2.5:72b"
    GPT_4O_MINI = "gpt-4o-mini"
    GPT_4O = "gpt-4o"
    O3_MINI = "o3-mini"
    O4_MINI = "o4-mini"
    O1 = "o1"
    GPT_4_1 = "gpt-4.1"
    GPT_4_1_MINI = "gpt-4.1-mini"
    GEMINI_2_FLASH = "gemini-2.0-flash"
    GEMINI_2_5_PRO = "gemini-2.5-pro-preview-03-25"
    CLAUDE_3_7_SONNET = "claude-3-7-sonnet-20250219"
    COMMAND_A = "command-a-03-2025"



class Embeddings:
    NOMIC_EMBED_TEXT = "nomic-embed-text" # 768
    TEXT_EMBEDDING_3_LARGE = "text-embedding-3-large" # up to 3072
    BGE_M3 = "bge-m3:latest" # 1024


# TODO: RWTH CORINTH not working:
# curl -X POST http://chat.corinth.informatik.rwth-aachen.de/api/chat/completions \
# -H "Authorization: Bearer sk-2f91eafb6356472681a1d63f33488658" \
# -H "Content-Type: application/json" \
# -d '{
#       "model": "llama3.1:70b",
#       "messages": [
#         {
#           "role": "user",
#           "content": "Why is the sky blue?"
#         }
#       ]
#     }'
# curl: (7) Failed to connect to chat.corinth.informatik.rwth-aachen.de port 80: Connection refused

# curl -X POST http://chat.corinth.informatik.rwth-aachen.de/api/chat/completions -H "Authorization: Bearer sk-2f91eafb6356472681a1d63f33488658" -H "Content-Type: application/json" -d '{"model":"llama3.1:70b","messages":[{"role":"user","content":"Why is the sky blue?"}]}'


# {
#    "data":[
#       {
#          "type":"model",
#          "id":"claude-3-7-sonnet-20250219",
#          "display_name":"Claude 3.7 Sonnet",
#          "created_at":"2025-02-24T00:00:00Z"
#       },
#       {
#          "type":"model",
#          "id":"claude-3-5-sonnet-20241022",
#          "display_name":"Claude 3.5 Sonnet (New)",
#          "created_at":"2024-10-22T00:00:00Z"
#       },
#       {
#          "type":"model",
#          "id":"claude-3-5-haiku-20241022",
#          "display_name":"Claude 3.5 Haiku",
#          "created_at":"2024-10-22T00:00:00Z"
#       },
#       {
#          "type":"model",
#          "id":"claude-3-5-sonnet-20240620",
#          "display_name":"Claude 3.5 Sonnet (Old)",
#          "created_at":"2024-06-20T00:00:00Z"
#       },
#       {
#          "type":"model",
#          "id":"claude-3-haiku-20240307",
#          "display_name":"Claude 3 Haiku",
#          "created_at":"2024-03-07T00:00:00Z"
#       },
#       {
#          "type":"model",
#          "id":"claude-3-opus-20240229",
#          "display_name":"Claude 3 Opus",
#          "created_at":"2024-02-29T00:00:00Z"
#       }
#    ],
#    "has_more":false,
#    "first_id":"claude-3-7-sonnet-20250219",
#    "last_id":"claude-3-opus-20240229"
# }