from abc import ABC, abstractmethod
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import StateGraph
from langchain_core.runnables.graph import MermaidDrawMethod
from utils.utils import is_async_context
from .config import BaseGraphConfig

class BaseGraph(ABC):
    """A class to represent an agent graph.
    """

    def __init__(self, config: BaseGraphConfig):
        if not config:
            raise ValueError("config is required")
        
        self.config = config        

    @abstractmethod
    def create_graph(self, use_async: bool) -> StateGraph:
        """Create the agent graph for the given class instance"""
        pass

    def compile_workflow(self, checkpointer: InMemorySaver = None):
        graph = self.create_graph(use_async=is_async_context())

        if checkpointer:
            return graph.compile(checkpointer=checkpointer)
        return graph.compile()
    
    def generate_graph_image(self, workflow, image_path: str = "graph_output.png"):
        try:
            graph_png = workflow.get_graph().draw_mermaid_png(
                draw_method=MermaidDrawMethod.API
            )

            with open(image_path, "wb") as f:
                f.write(graph_png)
        except Exception as e:
            print("Error in generating graph image:", e)