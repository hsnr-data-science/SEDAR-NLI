import json
from cache.cacheable import CacheableRegistry

class MinimalEncoder(json.JSONEncoder):
    def default(self, obj):
        # Handle lists and dictionaries recursively
        if isinstance(obj, (list, tuple)):
            return [self.default(item) for item in obj]
        elif isinstance(obj, dict):
            return {key: self.default(value) for key, value in obj.items()}

        # Handle cacheable objects
        if CacheableRegistry.is_cacheable(obj):
            return self._serialize_cacheable_object(obj)

        # Fallback for other objects: use string representation
        return str(obj)

    def _serialize_cacheable_object(self, obj):
        """
        Serialize cacheable objects in the format: ClassName(id=..., name=..., title=...).
        """
        # Collect attributes for cacheable objects
        attributes = {}
        if hasattr(obj, 'id'):
            attributes['id'] = getattr(obj, 'id')
        if hasattr(obj, 'name'):
            attributes['name'] = getattr(obj, 'name')
        if hasattr(obj, 'title'):
            attributes['title'] = getattr(obj, 'title')
        if hasattr(obj, 'link'):
            attributes['link'] = getattr(obj, 'link')

        # Format as ClassName(attr=value, ...)
        class_name = obj.__class__.__name__
        if attributes:
            attrs_str = ', '.join(f'{key}={repr(value)}' for key, value in attributes.items())
            return f"{class_name}({attrs_str})"
        else:
            return f"{class_name}()"

class ExtendedEncoder(json.JSONEncoder):
    def __init__(self, *args, max_depth=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.max_depth = max_depth

    def default(self, obj, _current_depth=0):
        # If max depth is defined and exceeded, return a simplified representation
        if self.max_depth is not None and _current_depth >= self.max_depth:
            return f"<Truncated object at depth {self.max_depth}>"

        if hasattr(obj, 'content') and isinstance(obj.content, dict):
            return self._serialize_dict(obj.content, _current_depth)

        elif hasattr(obj, '__dict__'):
            return self._serialize_object(obj, _current_depth)

        elif isinstance(obj, (set, frozenset)):
            return list(obj)

        try:
            return super().default(obj)
        except TypeError:
            return str(obj)

    def _serialize_object(self, obj, _current_depth):
        result = {}
        for key, value in obj.__dict__.items():
            if not key.startswith('_'):  # Skip private attributes
                result[key] = self.default(value, _current_depth=_current_depth + 1)
        result['__class__'] = obj.__class__.__name__
        return result

    def _serialize_dict(self, obj, _current_depth):
        return {
            key: self.default(value, _current_depth=_current_depth + 1)
            for key, value in obj.items()
        }