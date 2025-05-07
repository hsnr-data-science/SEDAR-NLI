from abc import ABC, abstractmethod
from langsmith.schemas import Example, Run

class SequenceEvaluator(ABC):

    MAX_SEQUENCE_LENGTH = 200

    def __init__(self, metric_prefix: str):
        self.metric_prefix = metric_prefix

    @abstractmethod
    def extract_sequences(self, example: Example, run: Run) -> tuple[list[str], list[str]]:
        pass

    def _compute_list_edit_distance(self, list1: list[str], list2: list[str]) -> int:
        n = len(list1)
        m = len(list2)

        dp = [[0 for _ in range(m+1)] for _ in range(n+1)]

        for i in range(n+1):
            for j in range(m+1):
                if i == 0:
                    dp[i][j] = j
                elif j == 0:
                    dp[i][j] = i
                elif list1[i-1] == list2[j-1]:
                    dp[i][j] = dp[i-1][j-1]
                else:
                    dp[i][j] = 1 + min(dp[i-1][j], dp[i][j-1], dp[i-1][j-1])

        return dp[n][m]
    
    def _compute_longest_common_subsequence(self, list1: list[str], list2: list[str]) -> int:
        n = len(list1)
        m = len(list2)

        dp = [[0 for _ in range(m+1)] for _ in range(n+1)]

        for i in range(1, n+1):
            for j in range(1, m+1):
                if list1[i-1] == list2[j-1]:
                    dp[i][j] = 1 + dp[i-1][j-1]
                else:
                    dp[i][j] = max(dp[i-1][j], dp[i][j-1])

        return dp[n][m]
    
    def evaluate_exact_match(self, run: Run, example: Example) -> dict:
        if run.outputs["has_errored"]:
            return {"key": f"{self.metric_prefix}_exact_match", "score": 0}

        example_sequence, run_sequence = self.extract_sequences(example, run)

        return {"key": f"{self.metric_prefix}_exact_match", "score": int(example_sequence == run_sequence)}

    def evaluate_edit_distance(self, run: Run, example: Example) -> dict:
        if run.outputs["has_errored"]:
            return {"key": f"{self.metric_prefix}_edit_distance", "score": 1}

        example_sequence, run_sequence = self.extract_sequences(example, run)

        max_length = max(len(example_sequence), len(run_sequence))
        edit_distance = self._compute_list_edit_distance(example_sequence, run_sequence)
        normalized_edit_distance = edit_distance / max_length if max_length > 0 else 0

        return {"key": f"{self.metric_prefix}_edit_distance", "score": normalized_edit_distance}
    
    def evaluate_longest_common_subsequence(self, run: Run, example: Example) -> dict:
        if run.outputs["has_errored"]:
            return {"key": f"{self.metric_prefix}_longest_common_subsequence", "score": 0}

        example_sequence, run_sequence = self.extract_sequences(example, run)

        max_length = max(len(example_sequence), len(run_sequence))
        lcs = self._compute_longest_common_subsequence(example_sequence, run_sequence)
        normalized_lcs = lcs / max_length if max_length > 0 else 0

        return {"key": f"{self.metric_prefix}_longest_common_subsequence", "score": normalized_lcs}

    def evaluate_jaccard_similarity(self, run: Run, example: Example) -> dict:
        if run.outputs["has_errored"]:
            return {"key": f"{self.metric_prefix}_jaccard_similarity", "score": 0}

        example_sequence, run_sequence = self.extract_sequences(example, run)
        example_set = set(example_sequence)
        run_set = set(run_sequence)

        intersection = example_set & run_set
        union = example_set | run_set

        jaccard_similarity = len(intersection) / len(union) if len(union) > 0 else 0

        return {"key": f"{self.metric_prefix}_jaccard_similarity", "score": jaccard_similarity}
    
    def evaluate_extra_steps(self, run: Run, example: Example) -> dict:
        """Higher is better - normalized inverse of extra steps."""
        if run.outputs["has_errored"]:
            return {"key": f"{self.metric_prefix}_extra_steps", "score": 0}

        example_sequence, run_sequence = self.extract_sequences(example, run)

        extra_steps = max(len(run_sequence) - len(example_sequence), 0)
        normalized_brevity_score = 1 - (extra_steps / self.MAX_SEQUENCE_LENGTH)
        normalized_brevity_score = max(0.0, normalized_brevity_score)  # Clamp to [0, 1]

        return {"key": f"{self.metric_prefix}_extra_steps", "score": normalized_brevity_score}

    def evaluate_unmatched_steps(self, run: Run, example: Example) -> dict:
        """Higher is better — normalized inverse of unmatched steps."""
        if run.outputs["has_errored"]:
            return {"key": f"{self.metric_prefix}_unmatched_steps", "score": 0}

        example_sequence, run_sequence = self.extract_sequences(example, run)

        i = j = 0
        unmatched_steps = 0

        while i < len(example_sequence) and j < len(run_sequence):
            if example_sequence[i] == run_sequence[j]:
                i += 1
            else:
                unmatched_steps += 1
            j += 1

        unmatched_steps += len(run_sequence) - j

        normalized_score = 1 - (unmatched_steps / self.MAX_SEQUENCE_LENGTH)
        normalized_score = max(0.0, normalized_score)  # Clamp to [0, 1]

        return {"key": f"{self.metric_prefix}_unmatched_steps", "score": normalized_score}
    
    def get_all_evals(self):
        return [
            self.evaluate_edit_distance,
            self.evaluate_exact_match,
            self.evaluate_longest_common_subsequence,
            self.evaluate_jaccard_similarity,
            self.evaluate_extra_steps,
            self.evaluate_unmatched_steps
        ]