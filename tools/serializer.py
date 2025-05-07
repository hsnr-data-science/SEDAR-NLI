from typing import Any
from cache.cacheable import CacheableRegistry

class CacheableSerializer:
    """
    Handles the serialization of cacheable objects, lists, or dictionaries.
    """

    @classmethod
    def serialize_result(cls, result: Any) -> Any:
        if cls.is_serializable(result):
            return cls.serialize(result)
        return result

    @classmethod
    def is_serializable(cls, result: Any) -> bool:
        return (
            CacheableRegistry.is_cacheable(result) or
            cls.is_serializable_list(result) or
            cls.is_serializable_dict(result)
        )

    @classmethod
    def is_serializable_list(cls, result: Any) -> bool:
        return isinstance(result, list) and result and CacheableRegistry.is_cacheable(result[0])

    @classmethod
    def is_serializable_dict(cls, result: Any) -> bool:
        return isinstance(result, dict) and result and CacheableRegistry.is_cacheable(next(iter(result.values())))

    @classmethod
    def serialize(cls, result: Any) -> Any:
        return CacheableRegistry.serialize(result)