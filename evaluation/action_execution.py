from langsmith.schemas import Example, Run

class ActionExecutionEvaluator:

    def _get_number_of_executions_per_query(self, run: Run) -> dict:
        executions_per_query = dict()

        for message in run.outputs["messages"]:
            if message.source_node == "tool_agent" or message.source_node == "code_agent":
                if message.query_index not in executions_per_query:
                    executions_per_query[message.query_index] = 0

                executions_per_query[message.query_index] += 1

        return executions_per_query

    def evaluate_pass_at_1(self, run: Run, example: Example) -> dict:
        if run.outputs["has_errored"]:
            return {"key": "pass@1", "score": 0}

        # For the example, pass@k should always be 1 => we only look at the run
        executions_per_query = self._get_number_of_executions_per_query(run)

        pass_at_1_values = [1 if executions == 1 else 0 for executions in executions_per_query.values()]

        if len(pass_at_1_values) == 0:
            return {"key": "pass@1", "score": 0}

        return {"key": "pass@1", "score": sum(pass_at_1_values) / len(pass_at_1_values)}
    
    def evaluate_pass_at_2(self, run: Run, example: Example) -> dict:
        if run.outputs["has_errored"]:
            return {"key": "pass@2", "score": 0}

        # For the example, pass@k should always be 1 => we only look at the run
        executions_per_query = self._get_number_of_executions_per_query(run)

        pass_at_2_values = [1 if executions <= 2 else 0 for executions in executions_per_query.values()]

        if len(pass_at_2_values) == 0:
            return {"key": "pass@2", "score": 0}

        return {"key": "pass@2", "score": sum(pass_at_2_values) / len(pass_at_2_values)}