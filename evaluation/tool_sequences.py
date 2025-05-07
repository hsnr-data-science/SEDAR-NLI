from langsmith.schemas import Example, Run
from .sequences import SequenceEvaluator

class ToolSequenceEvaluator(SequenceEvaluator):

    def extract_sequences(self, example: Example, run: Run) -> tuple[list[str], list[str]]:
        example_sequence = self.extract_example_sequence(example)
        run_sequence = []

        for message in run.outputs["messages"]:
            has_added_tool_call = False

            if message.source_node == "tool_agent":
                if message.tool_calls:
                    run_sequence.append(message.tool_calls[0]["name"])
            elif message.source_node == "code_agent":
                for tool_call in example_sequence:
                    if tool_call in message.content: # We might have to keep the tool calls in the order of the code snippet
                        run_sequence.append(tool_call)
                        has_added_tool_call = True
                
                if not has_added_tool_call:
                    run_sequence.append("code_placeholder")

        return example_sequence, run_sequence

    def extract_example_sequence(self, example: Example) -> list[str]:
        return [message["tool_calls"][0]["name"] for message in example.outputs["messages"] if "tool_calls" in message and message["tool_calls"] and message["source_node"] == "tool_agent"]