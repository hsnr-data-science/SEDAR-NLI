import chainlit as cl
from typing import Literal
from langgraph.graph import StateGraph, START, END
from .base_graph import BaseGraph
from .config import BaseGraphConfig
from tools.tool_retrieval import ToolRetriever
from states.consts import TOOL_ACTION, CODE_ACTION, CONTINUE_ACTION, ERROR_ACTION, DECLINE_ACTION
from states.sedar_agent_state import SedarAgentState
from agents.sedar.manager_agent import ManagerAgent
from agents.sedar.code_agent import CodeAgent
from agents.sedar.synthesize_agent import SynthesizeAgent
from tools.code_tool import CodeExecutionTool
from tools.sedar_tool_message import SedarToolMessage
from .tool_graph import ToolGraph

class SedarAgentGraph(BaseGraph):

    def __init__(self, config: BaseGraphConfig, tool_retriever: ToolRetriever):
        super().__init__(config)
        self.tool_retriever = tool_retriever

    def create_graph(self, use_async = False) -> StateGraph:
        graph = StateGraph(SedarAgentState)

        graph.add_node(
            "manager_agent",
            lambda state: ManagerAgent(
                state=state,
                tool_retriever=self.tool_retriever,
                model_config=self.config.default_llm,
                prompt_compression=self.config.prompt_compression
            ).invoke()
        )

        graph.add_node(
            "code_agent",
            lambda state: CodeAgent(
                state=state,
                tool_retriever=self.tool_retriever,
                model_config=self.config.default_llm,
                prompt_compression=self.config.prompt_compression
            ).invoke()
        )

        graph.add_node(
            "tool_agent",
            self.ainvoke_tool_graph if use_async else self.invoke_tool_graph
        )

        graph.add_node(
            "code_execution_tool",
            lambda state: CodeExecutionTool(
                state=state
            ).run()
        )

        graph.add_node(
            "synthesize_agent",
            lambda state: SynthesizeAgent(
                state=state,
                tool_retriever=self.tool_retriever,
                model_config=self.config.default_llm,
                prompt_compression=self.config.prompt_compression
            ).invoke()
        )

        graph.add_node(
            "remove_tool_messages",
            self.remove_tool_messages
        )

        graph.add_node(
            "remove_code_messages",
            self.remove_code_messages
        )

        graph.add_edge(START, "manager_agent")
        graph.add_conditional_edges("manager_agent", self.next_action)

        if self.config.human_confirmation:
            graph.add_conditional_edges(
                "code_agent",
                self.acode_confirmation if use_async else self.code_confirmation
            )
            graph.add_conditional_edges("tool_agent", self.check_tool_cancellation)
        else:
            graph.add_edge("code_agent", "code_execution_tool")
            graph.add_edge("tool_agent", "synthesize_agent")

        graph.add_edge("remove_tool_messages", "manager_agent")
        graph.add_edge("remove_code_messages", "manager_agent")

        graph.add_edge("code_execution_tool", "synthesize_agent")
        graph.add_conditional_edges("synthesize_agent", self.run_query)

        return graph
    
    def next_action(self, state: SedarAgentState) -> Literal["tool_agent", "code_agent", "synthesize_agent", "manager_agent"]:
        if TOOL_ACTION in state["next_action"]:
            return "tool_agent"
        elif CODE_ACTION in state["next_action"]:
            return "code_agent"
        elif ERROR_ACTION in state["next_action"]: # If the manager output is invalid, try again
            return "manager_agent"
        else:
            return "synthesize_agent"

    # The actual ask for tool confirmation happens inside the tool_graph
    def check_tool_cancellation(self, state: SedarAgentState) -> Literal["synthesize_agent", "remove_tool_messages"]:
        last_message = state["messages"][-1]
        if TOOL_ACTION in state["next_action"] and not isinstance(last_message, SedarToolMessage):
            return "remove_tool_messages"
        return "synthesize_agent"
    
    async def acode_confirmation(self, state: SedarAgentState) -> Literal["code_execution_tool", "remove_code_messages"]:
        content = "Code execution step:\n"
        content += state["messages"][-1].content

        res = await cl.AskActionMessage(
            content=content,
            actions=[
                cl.Action(name="yes", payload={"value": "y"}, label="Yes ✅"),
                cl.Action(name="no", payload={"value": "n"}, label="No ❌")
            ]
        ).send()

        if res.get("payload").get("value") == "n":
            print("Code execution cancelled.")
            return "remove_code_messages"
        return "code_execution_tool"

    def code_confirmation(self, state: SedarAgentState) -> Literal["code_execution_tool", "remove_code_messages"]:
        print("Code execution step:")
        print(state["messages"][-1].content)
        
        user_confirmation = input("Proceed with the code execution? (y/n): ").strip().lower()
        if user_confirmation == "n":
            print("Code execution cancelled.")
            return "remove_code_messages"
        if user_confirmation != "y":
            print("Invalid input. Please enter 'y' or 'n'.")
            return self.code_confirmation(state)
        
        return "code_execution_tool"
    
    def run_query(self, state: SedarAgentState) -> Literal["manager_agent", END]:
        # If max retries are reached, we leave this subgraph
        if state["current_query_number_of_tries"] >= 3:
            return END

        if CONTINUE_ACTION in state["next_action"] or state["current_query_index"] < len(state["decomposed_queries"]) - 1:
            return "manager_agent"
        return END
    
    async def ainvoke_tool_graph(self, state: SedarAgentState):
        current_instance = state["current_instance"]
        tool_graph = ToolGraph(self.config, self.tool_retriever, current_instance)
        workflow = tool_graph.compile_workflow()
        # tool_graph.generate_graph_image(workflow, "tool_graph.png")
        return await workflow.ainvoke(state)
    
    def invoke_tool_graph(self, state: SedarAgentState):
        current_instance = state["current_instance"]
        tool_graph = ToolGraph(self.config, self.tool_retriever, current_instance)
        workflow = tool_graph.compile_workflow()
        # tool_graph.generate_graph_image(workflow, "tool_graph.png")
        return workflow.invoke(state)
    
    def remove_tool_messages(self, state: SedarAgentState):
        """
        This function is called when a tool call is declined by the user.
        It removes the last message and tool_agent_message from state.
        This needs to be done as the openAI API does not accept a tool call without the execution.
        """
        state["messages"].pop()
        state["tool_agent_messages"].pop()
        state["next_action"] = DECLINE_ACTION
        return state
    
    def remove_code_messages(self, state: SedarAgentState):
        """
        This function is called when a code execution is declined by the user.
        It removes the last message and code_agent_message from state.
        """
        state["messages"].pop()
        state["code_agent_messages"].pop()
        state["next_action"] = DECLINE_ACTION
        return state