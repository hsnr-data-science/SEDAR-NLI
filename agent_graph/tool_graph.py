import chainlit as cl
from typing import Literal, Any
from langgraph.graph import StateGraph, START, END
from langgraph.prebuilt import ToolNode
from agent_graph.config import MainGraphConfig
from .base_graph import BaseGraph
from states.sedar_agent_state import SedarAgentState
from tools.tools import ToolManager, update_tool_state
from tools.tool_retrieval import ToolRetriever
from agents.sedar.tool_agent import ToolAgent

class ToolGraph(BaseGraph):
    """
    This class represents a subgraph responsible for any tool calling.
    """
    def __init__(self, config: MainGraphConfig, tool_retriever: ToolRetriever, class_instance: Any):
        super().__init__(config)
        self.class_instance = class_instance
        self.class_type = class_instance.__class__
        self.tool_manager = ToolManager()
        self.tool_retriever = tool_retriever

    def create_graph(self, use_async = False) -> StateGraph:
        graph = StateGraph(SedarAgentState)
        tools = self.tool_manager.get_tools(self.class_instance, self.config.full_doc_strings)

        graph.add_node(
            "tool_agent",
            lambda state: ToolAgent(
                state=state,
                tool_retriever=self.tool_retriever,
                model_config=self.config.default_llm,
                prompt_compression=self.config.prompt_compression
            ).invoke(tools)
        )

        graph.add_node(
            "tools",
            ToolNode(tools)
        )

        graph.add_node(
            "update_tool_state",
            update_tool_state
        )

        graph.add_edge(START, "tool_agent")

        if self.config.human_confirmation:
            graph.add_conditional_edges(
                "tool_agent",
                self.atool_confirmation if use_async else self.tool_confirmation
            )
        else:
            graph.add_edge("tool_agent", "tools")

        graph.add_edge("tools", "update_tool_state")
        graph.add_edge("update_tool_state", END)

        return graph

    async def atool_confirmation(self, state: SedarAgentState) -> Literal["tools", END]:
        content = "Tool step:\n"
        content += state["messages"][-1].content
        content += "\nTool-Calls: " + str(state["messages"][-1].tool_calls)

        res = await cl.AskActionMessage(
            content=content,
            actions=[
                cl.Action(name="yes", payload={"value": "y"}, label="Yes ✅"),
                cl.Action(name="no", payload={"value": "n"}, label="No ❌")
            ]
        ).send()

        if res.get("payload").get("value") == "n":
            print("Tool step cancelled.")
            return END
        return "tools"
    
    def tool_confirmation(self, state: SedarAgentState) -> Literal["tools", END]:
        print("Tool step:")
        print(state["messages"][-1].content)
        print("Tool-Calls:", state["messages"][-1].tool_calls)

        user_confirmation = input("Proceed with the tool step? (y/n): ").strip().lower()

        if user_confirmation == "n":
            print("Tool step cancelled.")
            return END
        if user_confirmation != "y":
            print("Invalid input. Please enter 'y' or 'n'.")
            return self.tool_confirmation(state)
        
        return "tools"