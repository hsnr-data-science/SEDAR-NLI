import re
import uuid
import inspect
from typing import Any, Callable
from qdrant_client import QdrantClient
from qdrant_client.http import models
from langchain.retrievers.document_compressors import FlashrankRerank
from langchain.retrievers import ContextualCompressionRetriever
from langchain_qdrant import QdrantVectorStore
from langchain_core.documents import Document
from langchain_core.caches import InMemoryCache
from cache.cacheable import CacheableRegistry
from tools.sedar_tool import SedarTool
from models.config import ModelConfig
from models.models import get_model
from .consts import QDRANT_URL, QDRANT_COLLECTION
from utils.utils import get_function_details, get_minimal_docstring

LLMLINGUA_NO_COMPRESS = "<llmlingua, compress=False>"
LLMLINGUA_RATE = "<llmlingua, rate=0.6>"
LLMLINGUA_CLOSE = "</llmlingua>"
DOCSTRING_SECTIONS_TO_REMOVE = ["Description", "Notes", "Raises", "Example"]

class ToolRetriever:
    """
    A class to retrieve tools from the AI model.

    Attributes:
        embedding_config (ModelConfig): The model configuration for the embedding model.
        embedding (OllamaEmbeddings): The embedding model.
        qdrant_client (QdrantClient): The Qdrant client.
        vector_store (QdrantVectorStore): The Qdrant vector store.
    """

    def __init__(self, embedding_config: ModelConfig):
        self.embedding_config = embedding_config
        self.embedding = get_model(embedding_config)
        self.qdrant_client = QdrantClient(url=QDRANT_URL)
        self.cache = InMemoryCache()

        try:
            self.vector_store = QdrantVectorStore.from_existing_collection( # TODO: create collection if it doesn't exist
                embedding=self.embedding,
                collection_name=QDRANT_COLLECTION,
                url=QDRANT_URL
            )
        except Exception as e:
            print("Error in retrieving vector store:", e)
            print("Recreating collection...")
            self.qdrant_client.delete_collection(collection_name=QDRANT_COLLECTION)
            self.qdrant_client.create_collection(
                collection_name=QDRANT_COLLECTION,
                vectors_config=models.VectorParams(size=self.embedding_config.embedding_size, distance=models.Distance.COSINE)
            )
            self.vector_store = QdrantVectorStore.from_existing_collection(
                embedding=self.embedding,
                collection_name=QDRANT_COLLECTION,
                url=QDRANT_URL
            )

    def _invoke_rerank_retriever(self, search_kwargs: dict, query: str, k: int = 5) -> list[Document]:
        """
        Invokes a rerank retriever.

        Args:
            search_kwargs (dict): The search keyword arguments for the base retriever.
            query (str): The query to search for.
            k (int): The number of documents to return.

        Returns:
            list[Document]: A list of documents.
        """
        cached_results = self.cache.lookup(query, str(search_kwargs)+str(k))

        if cached_results is not None:
            return cached_results
        
        results = self._get_rerank_retriever(search_kwargs, k=k).invoke(input=query)
        self.cache.update(query, str(search_kwargs)+str(k), results)
        
        return results

    def _get_rerank_retriever(self, search_kwargs: dict, k: int = 5) -> ContextualCompressionRetriever:
        """
        Creates a rerank retriever.

        Args:
            search_kwargs (dict): The search keyword arguments for the base retriever.
            k (int): The number of documents to return.

        Returns:
            ContextualCompressionRetriever: A rerank retriever.
        """
        base_retriever = self.vector_store.as_retriever(search_type="similarity", search_kwargs=search_kwargs)
        compressor = FlashrankRerank(top_n=k, model="ms-marco-MiniLM-L-12-v2")
        return ContextualCompressionRetriever(base_compressor=compressor, base_retriever=base_retriever)
    
    def _create_document(self, method: Callable, cacheable_class: Any):
        """
        Creates a document for a method.

        Args:
            method (Callable): A method of the cacheable object.

        Returns:
            dict: A dictionary representing the document.
        """
        return Document(
            page_content=get_function_details(method),
            id=str(uuid.uuid4()),
            metadata={"name": method.__name__, "parent_class": cacheable_class.__name__}
        )
    
    def _search_by_class(self, query: str, cacheable_class_name: str, k: int = 5) -> list[Document]:
        """
        Retrieves the methods most similar to the query from a specific class.

        Args:
            query (str): The query to search for.
            cacheable_class_name (str): The name of the cacheable class.
            k (int): The number of methods to return.

        Returns:
            list[Document]: A list of documents representing the methods.
        """
        search_kwargs = {
            "k": 20,
            "filter": models.Filter(
                must=[
                    models.FieldCondition(
                        key="metadata.parent_class",
                        match=models.MatchValue(value=cacheable_class_name)
                    )
                ]
            )
        }
        return self._invoke_rerank_retriever(search_kwargs, query, k=k)
    
    def _wrap_method_with_llmlingua(self, method_content: str) -> str:
        """
        Wraps a method content with <llmlingua> tags, where the signature is not compressed and the rest is compressed.

        Args:
            method_content (str): The full method string, including signature, docstring, and body.

        Returns:
            str: The method content wrapped with <llmlingua> tags.
        """
        method_pattern = r"^(def .*?:)(.*)$"
        match = re.match(method_pattern, method_content, flags=re.DOTALL)

        if not match:
            raise ValueError("Invalid method content format")

        signature, rest = match.groups()
        signature = signature.strip()
        rest = rest.strip() if rest else ""

        wrapped_signature = f"{LLMLINGUA_NO_COMPRESS}{signature}{LLMLINGUA_CLOSE}"

        if not rest:
            return wrapped_signature

        wrapped_rest = f"{LLMLINGUA_RATE}\n{rest}\n{LLMLINGUA_CLOSE}"
        
        return f"{wrapped_signature}\n{wrapped_rest}"

    def rebuild_collection(self, cacheable_classes: list[Any]):
        """
        Removes the entire collection and then refills it with documents representing all
        public methods of the cacheable classes.

        Args:
            
        """
        self.qdrant_client.delete_collection(collection_name=QDRANT_COLLECTION)
        self.qdrant_client.create_collection(
            collection_name=QDRANT_COLLECTION,
            vectors_config=models.VectorParams(size=self.embedding_config.embedding_size, distance=models.Distance.COSINE)
        )

        documents = []

        for cacheable_class in cacheable_classes:
            for _, method in CacheableRegistry.get_methods(cacheable_class):
                if CacheableRegistry.should_use_method(method):
                    documents.append(self._create_document(method, cacheable_class))

        self.vector_store.add_documents(documents)

    def get_method_documents(self, query: str, k: int = 5, cacheable_class: Any = None) -> list[Document]:
        """
        Retrieves the methods most similar to the query.

        Args:
            query (str): The query to search for.
            k (int): The number of methods to return.
            cacheable_class (Any): The cacheable class.

        Returns:
            list[Document]: A list of documents representing the methods.
        """
        if cacheable_class is not None and CacheableRegistry.is_cacheable(cacheable_class):
            return self._search_by_class(query, cacheable_class.__name__, k)
        
        return self._invoke_rerank_retriever({"k": 20}, query, k=k)
    
    def get_method_descriptions(self, query: str, k: int = 5, cacheable_class: Any = None) -> str:
        """
        Retrieves the descriptions of the methods most similar to the query.

        Args:
            query (str): The query to search for.
            k (int): The number of methods to return.
            cacheable_class (Any): The cacheable class.

        Returns:
            str: A string containing the descriptions of the methods.
        """
        documents = self.get_method_documents(query, k, cacheable_class)
        return "\n\n".join([get_minimal_docstring(method_doc.page_content, DOCSTRING_SECTIONS_TO_REMOVE) for method_doc in documents])
    
    def get_class_and_method_descriptions(
        self,
        query: str,
        cacheable_class: Any = None,
        k: int = 3,
        include_remaining_methods: bool = False,
        describe_all_classes: bool = False,
        compress_prompt: bool = False
    ) -> str:
        """
        Retrieves the description of the cacheable class and its retrieved methods most similar to the query.

        Args:
            query (str): The query to search for.
            cacheable_class (Any): The cacheable class.
            k (int): The number of methods to return.
            include_remaining_methods (bool): Whether to include remaining methods for each class (i.e. methods not retrieved).
            describe_all_classes (bool): Whether to describe all classes (i.e. also the ones for which no methods were retrieved).
            compress_prompt (bool): Whether to handle prompt compression.

        Returns:
            str: A string containing the description of the cacheable class.
        """
        if cacheable_class:
            return self._describe_cacheable_class(query, cacheable_class, k, include_remaining_methods, compress_prompt)
        return self._describe_retrieved_classes(query, k, include_remaining_methods, describe_all_classes, compress_prompt)

    def _describe_cacheable_class(
        self, query: str, cacheable_class: Any, k: int, include_remaining_methods: bool, compress_prompt: bool
    ) -> str:
        """Generate the description for a specific cacheable class."""
        retrieved_class = (
            (LLMLINGUA_RATE if compress_prompt else "")
            + f"class {cacheable_class.__name__}\n{cacheable_class.__doc__}\n"
            + (LLMLINGUA_CLOSE if compress_prompt else "")
            + self.get_method_descriptions(query, k, cacheable_class)
        )

        if include_remaining_methods:
            retrieved_class += "\n\n" + self._get_remaining_methods(cacheable_class, retrieved_class, compress_prompt)

        return retrieved_class

    def _describe_retrieved_classes(
        self, query: str, k: int, include_remaining_methods: bool, describe_all_classes: bool, compress_prompt: bool
    ) -> str:
        """Generate descriptions for all relevant classes."""
        retrieved_method_docs = self.get_method_documents(query, k=k)
        cacheable_classes = {}

        for doc in retrieved_method_docs:
            cacheable_classes.setdefault(doc.metadata["parent_class"], []).append(doc)

        class_descriptions = "\n\n".join(
            self._describe_single_class(class_name, docs, include_remaining_methods, compress_prompt)
            for class_name, docs in cacheable_classes.items()
        )

        if describe_all_classes:
            return class_descriptions + "\n\n" + self._describe_unqueried_classes(cacheable_classes, include_remaining_methods, compress_prompt)

        return class_descriptions

    def _describe_single_class(
        self, class_name: str, docs: list, include_remaining_methods: bool, compress_prompt: bool
    ) -> str:
        """Generate a description for a single class and its methods."""
        cls = CacheableRegistry.get_cacheable_class(class_name)
        if not cls:
            return ""

        class_description = f"class {cls.__name__}\n{cls.__doc__}\n"
        if compress_prompt:
            class_description = f"{LLMLINGUA_RATE}{class_description}{LLMLINGUA_CLOSE}"

        class_description += "\n\n".join(
            self._wrap_method_with_llmlingua(get_minimal_docstring(doc.page_content, DOCSTRING_SECTIONS_TO_REMOVE))
            if compress_prompt else get_minimal_docstring(doc.page_content, DOCSTRING_SECTIONS_TO_REMOVE)
            for doc in docs
        )

        if include_remaining_methods:
            retrieved_method_names = [doc.metadata["name"] for doc in docs]
            remaining_methods = self._get_remaining_methods(cls, retrieved_method_names, compress_prompt)
            class_description += "\n\n" + remaining_methods

        return class_description

    def _describe_unqueried_classes(
        self, queried_classes: dict, include_remaining_methods: bool, compress_prompt: bool
    ) -> str:
        """Generate descriptions for classes that were not part of the query."""
        all_classes = CacheableRegistry.get_cacheable_classes()
        unqueried_classes = [cls for cls in all_classes if cls.__name__ not in queried_classes]

        return "\n\n".join(
            self._describe_single_class(cls.__name__, [], include_remaining_methods, compress_prompt)
            for cls in unqueried_classes
        )

    def _get_remaining_methods(self, cls: Any, retrieved_method_names: list, compress_prompt: bool) -> str:
        """Get methods of the class not included in the retrieved method descriptions."""
        class_members = CacheableRegistry.get_methods(cls)
        remaining_methods = [
            f"def {method.__name__}{str(inspect.signature(method))}"
            for _, method in class_members
            if CacheableRegistry.should_use_method(method) and method.__name__ not in retrieved_method_names
        ]
        result = "\n".join(remaining_methods)
        if compress_prompt:
            result = f"{LLMLINGUA_NO_COMPRESS}{result}{LLMLINGUA_CLOSE}"

        return result
    
    def get_methods_for_cacheables(self, query: str, cacheable_instances: list[Any], k: int = 3) -> str:
        """
        Retrieves the methods most similar to the query for all cacheable classes.

        Args:
            query (str): The query to search for.
            cacheable_instances (list[Any]): Instances of cacheable classes.
            k (int): The number of methods to return.

        Returns:
            str: A string containing the methods for all cacheable classes.
        """
        full_cacheables_description = ""

        for cacheable_instance in cacheable_instances:
            cacheable_class = cacheable_instance.__class__
            full_cacheables_description += f"{cacheable_class.__name__}\n{cacheable_class.__doc__}\n"
            full_cacheables_description += self.get_method_descriptions(query, k, cacheable_class)
            full_cacheables_description += "\n"

        return full_cacheables_description

    def get_tools(self, query: str, tools: list[SedarTool], k: int = 5) -> list[SedarTool]:
        """
        Retrieves the tools most similar to the query.
        We pass in all tools for some cacheable object, and we return the tools that match the query.

        Args:
            query (str): The query to search for.
            tools (list[SedarTool]): Tools for a cacheable object.
            k (int): The number of tools to return.

        Returns:
            list[SedarTool]: A list of tools.
        """
        if not tools:
            return []

        documents = self.get_method_documents(query, k=k, cacheable_class=tools[0].class_instance.__class__)
        return [tool for tool in tools if tool.name in [doc.metadata["name"] for doc in documents]]