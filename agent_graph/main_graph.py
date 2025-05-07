from langgraph.graph import StateGraph, START, END
from typing import Literal
from .base_graph import BaseGraph
from states.agent_graph_state import AgentGraphState
from states.sedar_agent_state import get_state
from tools.tool_retrieval import ToolRetriever
from states.consts import CONTINUE_ACTION
from agents.main.query_decompose_agent import QueryDecomposeAgent
from agents.main.final_response_agent import FinalResponseAgent
from evaluation.eval_serializer import EvalJSONSerializer
from langgraph.checkpoint.memory import InMemorySaver
from .sedar_agent_graph import SedarAgentGraph
from .config import MainGraphConfig

class MainGraph(BaseGraph):

    def __init__(self, config: MainGraphConfig):
        super().__init__(config)
        self.tool_retriever = ToolRetriever(embedding_config=config.embedding_model)

    def create_graph(self, use_async = False) -> StateGraph:
        graph = StateGraph(AgentGraphState)

        graph.add_node(
            "query_decompose_agent",
            lambda state: QueryDecomposeAgent(
                state=state,
                tool_retriever=self.tool_retriever,
                model_config=self.config.default_llm,
                prompt_compression=self.config.prompt_compression
            ).invoke()
        )

        graph.add_node(
            "sedar_agent",
            self.ainvoke_sedar_agent_graph if use_async else self.invoke_sedar_agent_graph
        )

        graph.add_node(
            "final_response_agent",
            lambda state: FinalResponseAgent(
                state=state,
                tool_retriever=self.tool_retriever,
                model_config=self.config.default_llm,
                prompt_compression=self.config.prompt_compression
            ).invoke()
        )

        graph.add_edge(START, "query_decompose_agent")
        graph.add_edge("query_decompose_agent", "sedar_agent")
        graph.add_edge("sedar_agent", "final_response_agent")
        graph.add_edge("final_response_agent", END)

        return graph

    async def ainvoke_sedar_agent_graph(self, state: AgentGraphState):
        sedar_agent_state = get_state(state["user_query"], state["decomposed_queries"], state["sedar_agent_state"])
        sedar_agent_graph = SedarAgentGraph(self.config, self.tool_retriever)

        checkpointer = InMemorySaver(serde=EvalJSONSerializer())
        workflow = sedar_agent_graph.compile_workflow(checkpointer=checkpointer)
        config = {"configurable": {"thread_id": "1"}, "recursion_limit": 50}

        # sedar_agent_graph.generate_graph_image(workflow, "sedar_agent_graph.png")
        try:
            sedar_agent_state = await workflow.ainvoke(sedar_agent_state, config)
            state["messages"] = sedar_agent_state["messages"]
            state["has_errored"] = False

            # Error case
            if sedar_agent_state["next_action"] == CONTINUE_ACTION:
                state["has_errored"] = True
        except RecursionError:
            checkpoint_state = await workflow.get_state(config)
            state["has_errored"] = True
            state["messages"] = checkpoint_state.values["messages"]
        finally:
            state["sedar_agent_state"] = sedar_agent_state

        return state
    
    def invoke_sedar_agent_graph(self, state: AgentGraphState):
        sedar_agent_state = get_state(state["user_query"], state["decomposed_queries"], state["sedar_agent_state"])
        sedar_agent_graph = SedarAgentGraph(self.config, self.tool_retriever)

        checkpointer = InMemorySaver(serde=EvalJSONSerializer())
        workflow = sedar_agent_graph.compile_workflow(checkpointer=checkpointer)
        config = {"configurable": {"thread_id": "1"}, "recursion_limit": 50}

        # sedar_agent_graph.generate_graph_image(workflow, "sedar_agent_graph.png")
        try:
            sedar_agent_state = workflow.invoke(sedar_agent_state, config)
            state["messages"] = sedar_agent_state["messages"]
            state["has_errored"] = False

            # Error case
            if sedar_agent_state["next_action"] == CONTINUE_ACTION:
                state["has_errored"] = True
        except RecursionError:
            checkpoint_state = workflow.get_state(config)
            state["has_errored"] = True
            state["messages"] = checkpoint_state.values["messages"]
        finally:
            state["sedar_agent_state"] = sedar_agent_state

        return state