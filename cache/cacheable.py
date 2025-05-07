import inspect

class CacheableRegistry:
    _cacheable_classes = set()
    # This is to add custom tools to a cacheable class from the outside
    _registered_methods = dict()

    @classmethod
    def register(cls, cacheable_class):
        cls._cacheable_classes.add(cacheable_class)

    @classmethod
    def register_method(cls, target_class, method_name, method_func):
        """
        Register a method to be added to a specific cacheable class.
        """
        if target_class not in cls._registered_methods:
            cls._registered_methods[target_class] = dict()
        cls._registered_methods[target_class][method_name] = method_func

    @classmethod
    def get_registered_methods(cls, target_class):
        """
        Get all methods registered for a specific class.
        """
        return cls._registered_methods.get(target_class, {})

    @classmethod
    def get_all_registered_methods(cls):
        """
        Get all registered methods for all classes.
        """
        return cls._registered_methods
    
    @classmethod
    def get_methods(cls, cacheable_class):
        """
        Returns an iterable of all methods (both class-defined and registered) for a given cacheable class.
        """
        class_methods = inspect.getmembers(cacheable_class, predicate=inspect.isfunction)

        return list(class_methods)

    @classmethod
    def is_cacheable(cls, obj_or_type):
        """
        Check if a class or an instance is cacheable.
        """
        # If it's a type, compare directly
        if isinstance(obj_or_type, type):
            return obj_or_type in cls._cacheable_classes or any(
                issubclass(obj_or_type, cacheable_class)
                for cacheable_class in cls._cacheable_classes
            )
        
        # Otherwise, assume it's an instance
        return obj_or_type.__class__ in cls._cacheable_classes or any(
            issubclass(obj_or_type.__class__, cacheable_class)
            for cacheable_class in cls._cacheable_classes
        )
    
    @classmethod
    def get_cacheable_classes(cls):
        return list(cls._cacheable_classes)
    
    @classmethod
    def get_cacheable_class(cls, class_name):
        for cacheable_class in cls._cacheable_classes:
            if cacheable_class.__name__ == class_name:
                return cacheable_class
        return None
    
    @classmethod
    def serialize(cls, obj):
        """
        Serialize a cacheable object or nested structures containing cacheable objects.
        Currently it handles lists, dictionaries, and cacheable objects.
        """
        if isinstance(obj, list):
            return [cls.serialize(item) for item in obj]

        if isinstance(obj, dict):
            return {key: cls.serialize(value) for key, value in obj.items()}

        if cls.is_cacheable(obj):
            if hasattr(obj, 'content') and isinstance(obj.content, dict):
                return obj.content

        return obj
    
    @classmethod
    def should_use_method(cls, method):
        return method.__name__[0] != "_" and not getattr(method, "_exclude_from_cacheable", False)

    @classmethod
    def ensure_methods(cls):
        for cacheable_class in cls._cacheable_classes:
            methods_to_add = cls.get_registered_methods(cacheable_class)
            for method_name, method_func in methods_to_add.items():
                setattr(cacheable_class, method_name, method_func)
     
# Decorator to register API classes
def cacheable(cls):
    CacheableRegistry.register(cls)
    return cls

# Decorator to exclude methods from API classes
def exclude_from_cacheable(func):
    func._exclude_from_cacheable = True
    return func