from langchain.chat_models.base import BaseChatModel
from langsmith.schemas import Example, Run
from typing_extensions import TypedDict, Annotated
from models.config import ModelConfig
from models.models import get_model

grade_system_prompt = """You are a teacher grading a quiz.

You will be given a QUESTION, the GROUND TRUTH (correct) RESPONSE, and the STUDENT RESPONSE.

Here is the grade criteria to follow:
(1) Grade the student responses based ONLY on their factual accuracy relative to the ground truth answer.
(2) It is OK if the student response contains more information than the ground truth response, as long as it is factually accurate relative to the  ground truth response.

Correctness:
True means that the student's response meets all of the criteria and is mostly or exactly correct and it is complete.
False means that the student's response does not meet all of the criteria and is mostly or exactly incorrect.

Don't be too strict. Also, if there are some additional details in the student response, but the core information is correct, you should still mark it as correct.
But at least the key information should be present.

Explain your reasoning in a step-by-step manner to ensure your reasoning and conclusion are correct."""

# LLM-as-judge output schema
class Grade(TypedDict):
    """Compare the expected and actual answers and grade the actual answer."""
    reasoning: Annotated[str, ..., "Explain your reasoning for whether the actual response is correct or not."]
    is_correct: Annotated[bool, ..., "True if the student response is mostly or exactly correct, otherwise False."]

class LLMJudgeEvaluator:

    def __init__(self, model_config: ModelConfig):
        self.model_config = model_config
        self.llm: BaseChatModel = get_model(model_config).with_structured_output(Grade, method='function_calling')

    def _invoke_llm(self, user_message: str) -> Grade:
        return self.llm.invoke([{"role": "system", "content": grade_system_prompt}, {"role": "user", "content": user_message}])

    def evaluate_final_answer_correct(self, run: Run, example: Example) -> dict:
        if run.outputs["has_errored"]:
            return {"key": "is_correct", "score": 0}

        question = example.inputs["user_query"]
        reference_response = example.outputs["final_response"]
        student_response = run.outputs["final_response"]

        user_message = f"""
        QUESTION:
        {question}

        ========================================
        GROUND TRUTH RESPONSE:
        {reference_response}

        ========================================
        STUDENT RESPONSE:
        {student_response}
        """

        grade = self._invoke_llm(user_message)

        return {"key": "is_correct", "score": int(grade["is_correct"])}
        