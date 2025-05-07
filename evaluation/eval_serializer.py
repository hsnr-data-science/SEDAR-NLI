import copy
from typing import Any
from sedarapi.sedarapi import SedarAPI
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer, _msgpack_enc
from tools.sedar_tool_message import SedarToolMessage
from cache.cacheable import CacheableRegistry

class EvalJSONSerializer(JsonPlusSerializer):
    """Serializer for the eval JSON format."""

    def dumps_typed(self, obj: Any) -> tuple[str, bytes]:
        # Return empty if cacheable
        if CacheableRegistry.is_cacheable(obj):
            return super().dumps_typed({})

        if isinstance(obj, dict):
            if any(CacheableRegistry.is_cacheable(v) for v in obj.values()):
                return super().dumps_typed({})

        def clean_obj(data):
            if isinstance(data, SedarToolMessage):
                msg_dict = data.__dict__.copy()
                msg_dict.pop("raw_output", None)
                return msg_dict

            elif isinstance(data, dict):
                target_keys = {"user_query", "current_query", "messages"}
                if target_keys.issubset(data.keys()):
                    cleaned_messages = [
                        clean_obj(msg) for msg in data.get("messages", [])
                    ]
                    return {
                        "user_query": data["user_query"],
                        "current_query": data["current_query"],
                        "messages": cleaned_messages,
                    }
                else:
                    return {k: clean_obj(v) for k, v in data.items()}

            elif isinstance(data, list):
                return [clean_obj(item) for item in data]

            else:
                return data

        cleaned_obj = {}

        try:
            cleaned_obj = clean_obj(copy.deepcopy(obj))
        except Exception:
            pass

        try:
            return super().dumps_typed(cleaned_obj)
        except Exception:
            return super().dumps_typed({})
