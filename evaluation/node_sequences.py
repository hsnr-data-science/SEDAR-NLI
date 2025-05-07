from langsmith.schemas import Example, Run
from .sequences import SequenceEvaluator

class NodeSequenceEvaluator(SequenceEvaluator):

    def extract_sequences(self, example: Example, run: Run) -> tuple[list[str], list[str]]:
        return self.extract_example_sequence(example), self.extract_run_sequence(run)

    def extract_example_sequence(self, example: Example) -> list[str]:
        return [message["source_node"] for message in example.outputs["messages"]]
        
    def extract_run_sequence(self, run: Run) -> list[str]:
        return [message.source_node for message in run.outputs["messages"]]