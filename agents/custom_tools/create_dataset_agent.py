from langchain_core.messages import SystemMessage, HumanMessage
from prompts.prompts import create_dataset_system_prompt, create_dataset_prompt_template
from states.custom_tools.create_dataset_state import CreateDatasetState
from ..base_agent import BaseAgent

class CreateDatasetAgent(BaseAgent):

    def __init__(self, state: CreateDatasetState, model_config, prompt_compression: bool, source_node = "create_dataset_agent"):
        super().__init__(state, model_config, prompt_compression, source_node)
        self.prompt_template = create_dataset_prompt_template
    
    def _get_datasource_definition_examples(self) -> str:
        with open("./create_dataset_examples.json", "r") as f:
            return f.read()
    
    def _get_file_preview(self) -> str:
        filename = self.state["filename"]
        try:
            # NOTE: THIS HAS TO BE ./.files/ FOR THE CHATBOT OR ./data/ FOR THE EVALUATION
            with open(f"./data/{filename}", "r") as f:
                return "\n".join(f.readlines()[:5])
        except Exception as e:
            print(e)
            return ""

    def invoke(self, prompt: str = None):
        create_dataset_prompt = self._get_prompt_template(prompt).format(
            datasource_definition_examples=self._get_datasource_definition_examples(),
            query=self.state["user_query"],
            filename=self.state["filename"],
            file_preview=self._get_file_preview()
        )
        create_dataset_prompt = self._compress_prompt_if_needed(create_dataset_prompt)

        messages = [
            SystemMessage(content=create_dataset_system_prompt),
            *self._get_last_messages(),
            HumanMessage(content=create_dataset_prompt)
        ]

        llm = self.get_llm()
        ai_message = llm.invoke(messages)
        ai_message = self._add_metadata_to_message(ai_message)

        datasource_definition = self._parse_llm_response(ai_message.content)

        self.update_state("datasource_definition", datasource_definition)
        self.update_state("messages", [ai_message])

        return self.state