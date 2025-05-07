import re
import json
import datetime
from langsmith.client import Client

# example_function_call: {"name": "send_email", "parameters": {"recipient": "test@example.com", "subject": "Test", "message": "This is a test email."}}
# roles: system, user, assistant, ipython
TOOLS_TEMPLATE = """You have access to the following functions. To call a function, please respond with JSON for a function call.
Respond in the format {{"name": function name, "parameters": dictionary of argument name and its value}}.
Do not use variables.

{tools}
"""
SYSTEM_MESSAGE_TEMPLATE = "{tools}\n{system_message}"

class LangSmithChatLoader:

    def __init__(self, project_name: str, client: Client):
        self.project_name = project_name
        self.client = client

    def _get_traces(self):
        return {
            run.trace_id
            for run in self.client.list_runs(
                project_name=self.project_name,
                execution_order=1
            )
        }

    def get_llm_runs(self):
        successful_traces = self._get_traces()

        return [
            run
            for run in self.client.list_runs(
                project_name=self.project_name,
                run_type="llm",
                error=False
            )
            if run.trace_id in successful_traces
        ]
    
    def _format_system_message(self, content: str, tools: list) -> str:
        tool_content = TOOLS_TEMPLATE.format(tools="\n".join([json.dumps(tool, indent=4) for tool in tools])) if tools else ""
        return SYSTEM_MESSAGE_TEMPLATE.format(tools=tool_content, system_message=content).strip()
    
    def _format_tool_call(self, tool_call: dict) -> str:
        tool_call.pop("id")
        tool_call.pop("type")
        tool_call["parameters"] = tool_call.pop("args")
        return json.dumps(tool_call)
    
    def _remove_prompt_sections(self, content: str) -> str:
        query_decompose_example = "\n\n====================================================================================================\nFor example:\nUser query:\nAnnotate the Price column in the dataset \'Sales\' with a fitting Tag from the DBPedia ontology.\n\nAvailable classes and methods:\nclass SedarAPI:\n\"\"\"\nThe SedarAPI class provides an interface to interact with the SEDAR API. This is the main entry point for accessing the SEDAR API.\n\"\"\"\nMethods:\ndef get_current_user(self) -> User:\ndef get_user(self, user_id: str) -> User:\ndef get_mlflow_parameters(self) -> list[str]:\n\nclass Dataset:\n\"\"\"\nRepresents a dataset in the SEDAR system.\n\"\"\"\nMethods:\ndef get_all_attributes(self) -> list[Attribute]:\ndef publish(self, index=False, with_thread=True, profile=False) -> bool:\n\nclass Workspace:\n\"\"\"\nRepresents a workspace in the SEDAR system.\n\"\"\"\nMethods:\ndef ontology_annotation_search(self, search_term: str, ontology: Ontology = None) -> list[Annotation]:\ndef datasets_search(self, query: str) -> list[Dataset]:\ndef get_all_ontologies(self) -> list[Ontology]:\ndef get_favorite_datasets(self) -> list[Dataset]:\ndef dataset_create(self, user_query: str, filename: str):\n\nclass User:\n\"\"\"\nA class to represent a User in the workspace. This class provides methods to update and delete the user.\n\"\"\"\nMethods:\ndef update(self, new_email: str = None, firstname: str = None, lastname: str = None, username: str = None, is_admin: bool = None) -> User:\ndef delete(self):\n\nclass Attribute:\n\"\"\"\nRepresents an attribute/column/property of a dataset in the workspace. Contains methods to manage the attribute.\n\nAttributes:\n...\n\"\"\"\nMethods:\ndef annotate(self, ontology: Ontology, annotation: Annotation) -> dict:\n\nOutput:\n[\"Search for the dataset \'Sales\'\", \"Get all attributes of the \'Sales\' dataset and filter them to get the \'Price\' attribute\", \"Get all ontologies and find the DBPedia Ontology\", \"Perform the ontology annotation search with the search term \'Price\' and the ontology \'DBPedia\'\", \"Select a fitting tag from the annotations found\", \"Annotate the \'Price\' attribute from the \'Sales\' dataset with the fitting annotation\"]\n\nAnother example:\nUser query:\nWhat are the license details of the dataset \'Mathematics\'?\n\nclass Dataset:\n\"\"\"\nRepresents a dataset in the SEDAR system.\n\nAttributes:\ntitle (str): The name of the dataset.\ndescription (str): The description of the dataset.\nlicense (str): The license details of the dataset.\n\nMethods:\ndef get_preview_json(self) -> str:\ndef get_tags(self) -> list[Tag]:\n\nOutput:\n[\"Search for the dataset \'Mathematics\'\", \"Get the license details of the \'Mathematics\' dataset\"]\n\nAnother example:\nUser query:\nUse the \'test.csv\' file to create a new dataset called \'Test Dataset\'.\n\nOutput:\n[\"Create a dataset with the title \'Test Dataset\' based on the uploaded file \'test.csv\'\"]\n\n====================================================================================================\n\nNote: If you want to publish a dataset, it always needs to be ingested first. So, the ingestion is a step before publishing.\nNote: Always prefer dataset search to find one specific dataset.\nNote: When the task is to look at a dataset, often the JSON preview is helpful.\nNote: You don\'t need to get the current default workspace to answer a query, it\'s always already available."
        manager_example = "====================================================================================================\nExamples:\nExample 1:\n\"Get the user with the username \'johndoe\'.\"\n\nTool objects in cache:\n{\n    \"_WORKSPACE_dhf75h2n\": Workspace...\n}\n\nOther objects in cache:\n{\n    \"_d2nk15o1\": [{\"username\": \"janedoe\", \"firstname\": \"Jane\", \"lastname\": \"Doe\"}, {\"username\": \"johndoe\", \"firstname\": \"John\", \"lastname\": \"Doe\"},...]\n}\n\nAvailable classes and methods:\nclass User:\n\nAttributes:\n    firstname (str): The first name of the user.\n    username (str): The username of the user.\n\ndef update(self, ...) -> User:\n\nReasoning:\nWe can filter the users from the object cache by attribute \'username\'.\n\nOutput:\n{\n    \"action\": \"CODE\",\n    \"tool_object\": \"NONE\"\n}\n\nExample 2:\n\"Update the current user\'s name to \'Alice\'.\"\n\nTool objects in cache (containing current user):\n{\n    \"_USER_3n1k5jld\": User...\n}\n\nAvailable classes and methods:\nUser:\n    name (str): The name of the user.\n    email (str): The email address of the user.\n\ndef update(self, new_email: str = None, firstname: str = None, lastname: str = None, username: str = None, is_admin: bool = None) -> User:\ndef delete(self):\n\nReasoning:\nThe User class has a method to update the user\'s name.\n\nOutput:\n{\n    \"action\": \"TOOL\",\n    \"tool_object\": \"_USER_3n1k5jld\"\n}\n\nExample 3:\n\"What are the ontologies for the current workspace?\"\n\nTool objects in cache:\n{\n    \"_WORKSPACE_2j5ksfo3\": Workspace...\n    ...\n}\n\nAvailable classes and methods:\nWorkspace:\ndef get_all_ontologies(self) -> list[Ontology]:\ndef create_ontology(self, ...) -> Ontology:\n...\n\nReasoning:\nThe Workspace class has a method to retrieve all ontologies directly.\n\nOutput:\n{\n    \"action\": \"TOOL\",\n    \"tool_object\": \"_WORKSPACE_2j5ksfo3\"\n}\n\n====================================================================================================\n\n"
        synthesize_example = "====================================================================================================\n\nExample:\nUser query:\n\"What is the title of the current workspace?\"\n\nPrevious outputs:\nError: The current workspace object does not have a method to get the title.\n\nOutput:\nCONTINUE\n\nAnother example:\nUser query:\n\"List all users\"\n\nPrevious outputs:\n[{username: \"johndoe\", firstname: \"John\", lastname: \"Doe\"}, {username: \"janedoe\", firstname: \"Jane\", lastname: \"Doe\"}]\n\nOutput:\nThe users are: username: johndoe, firstname: John, lastname: Doe; username: janedoe, firstname: Jane, lastname: Doe\n\n====================================================================================================\n\n"
        search_example = "====================================================================================================\nHere is one example of a search query:\n\"Find all datasets about \'sales\'.\"\n\nOutput:\n{\"query\": \"sales\", \"advanced_search_parameters\": {}}\n\nAnother example:\n\"Show me all datasets created by testuser@example.com\"\n\nOutput:\n{\"query\": \"\", \"advanced_search_parameters\": {\"author\": \"testuser@example.com\"}}\n\nAnother example:\n\"Search for the \'Cars_Test_2\' dataset.\"\n\nOutput\n{\"query\": \"Cars_Test_2\", \"advanced_search_parameters\": {}}\n====================================================================================================\n\n"
        create_example = "\n\nHere is an example:\nUser query:\nUpload the file \'sales_data.csv\' and create a new dataset with the title \'Sales Data\'.\n\nUploaded file preview:\n\"ID,Date,Product,Price\n1,2022-01-01,Apple,2.5\n2,2022-01-02,Banana,1.5\n3,2022-01-03,Orange,3.0\"\n\nOutput:\n{\n    \"name\": \"Sales Data\",\n    \"id_column\": \"ID\",\n    \"read_type\": \"SOURCE_FILE\",\n    \"read_format\": \"csv\",\n    \"read_options\": {\n        \"delimiter\": \",\",\n        \"header\": \"true\",\n        \"inferSchema\": \"true\"\n    },\n    \"source_files\": [\n        \"sales_data\"\n    ],\n    \"write_type\": \"DELTA\"\n}"
        ml_example = "Here is an example:\nUser query:\nCreate a machine learning experiment for text similarity using the dataset \'sales_data\'. Use the column \"description\" as the target column.\n\nObject Cache:\n{\n    \"_DATASET_3n1k5jld\": Dataset(id=\"abc123xyz\"),\n    ...\n}\n\nOutput:\n{\n    \"library_name\": \"AutoGluon\",\n    \"datasets\": [\"_DATASET_3n1k5jld\"],\n    \"title\": \"Text Similarity Experiment\",\n    \"description\": \"Experiment to find text similarity\",\n    \"target_column\": \"description\",\n    \"data_type\": \"text\",\n    \"task_type\": \"similarity\",\n    \"problem_type\": \"similarity\",\n    \"user_params\": {},\n    \"is_public\": false,\n    \"include_llm_features\": false,\n    \"create_with_llm\": false\n}\n\n"
        label_example = "===============================\n\nHere is an example:\nDataset preview:\n{\n  \"body\": [\n    {\n      \"Model\": \"Sedan X1\",\n      \"PriceUSD\": \"$24,500\",\n      \"EngineType\": \"Gasoline\",\n      \"index\": 1\n    },\n    {\n      \"Model\": \"SUV Y2\",\n      \"PriceUSD\": \"$35,200\",\n      \"EngineType\": \"Diesel\",\n      \"index\": 2\n    },\n    {\n      \"Model\": \"Hatchback Z3\",\n      \"PriceUSD\": \"$18,700\",\n      \"EngineType\": \"Gasoline\",\n      \"index\": 3\n    }\n  ],\n  \"header\": [\n    \"Model\",\n    \"PriceUSD\",\n    \"EngineType\"\n  ]\n}\n\nAvailable labels in the ontology:\nautomobileModel\nprice\nAutomobileEngine\nDataset\nAutomobile\nGasoline\nHouse\nDiesel\n\nOutput:\n{\n    \"Model\": \"automobileModel\",\n    \"PriceUSD\": \"price\",\n    \"EngineType\": \"AutomobileEngine\"\n}\n\n===============================\n\n"

        if "Your task is to decompose the following user query into precise queries that" in content:
            content = content.replace(query_decompose_example, "")
            return content
        
        elif "Your task is to execute the following query using the available code" in content:
            lines = content.split("\n")  # Split content into lines

            # Find the index of the "Generate the code..." line
            start_index = next(i for i, line in enumerate(lines) if "Generate the code to fulfill the user's request:" in line)

            # Find the index of the "Output STRICTLY ONLY the Python code" line
            output_index = next(i for i in range(start_index, len(lines)) if "Output STRICTLY ONLY the Python code" in lines[i])

            # Keep the "Generate the code..." line and the next two lines (query + empty line)
            new_content = lines[:start_index + 3]  # Includes the necessary lines

            # Append the "Output STRICTLY ONLY the Python code" part
            new_content.append(lines[output_index])

            content = "\n".join(new_content) + "\n\nOutput code:\n"
            return content
        
        elif "You are a workflow coordinator responsible for managing agents and selecting" in content:
            content = content.replace(manager_example, "")
            return content
        
        elif "Your task is to decide if the user query can be answered based on previous outputs from the agents" in content:
            content = content.replace(synthesize_example, "")
            pattern = r"(or output CONTINUE if the query requires further processing\.\s*\n\n).*?\n\n(Output:)"
            content = re.sub(pattern, r"\1\2", content, flags=re.DOTALL)
            return content
        
        elif "Your task is to give a final answer to the intial user query based on the outputs from the other agents" in content:
            pattern = r"(Responses from other agents:\s*\n).*?\n\n(Now give a response to the user query based on the outcomes)"
            content = re.sub(pattern, r"\1\2", content, flags=re.DOTALL)
            content = content.replace("Responses from other agents:\n", "")
            return content
        
        elif "Your task is to search for datasets in the workspace based on the user query" in content:
            content = content.replace(search_example, "")
            return content
        
        elif "Your task is to create a new dataset in the workspace based on the user query and the uploaded file" in content:
            content = content.replace(create_example, "")
            return content

        elif "Your task is to create the right parameters to create an AutoML notebook based on the user query" in content:
            content = content.replace(ml_example, "")
            return content

        elif "Your task is to assign semantic labels from an ontology to a dataset based on the preview of the dataset" in content:
            content = content.replace(label_example, "")
            return content
        
        return content

    
    def get_sharegpt_format(self, llm_run):
        messages = llm_run.inputs["messages"][0]
        output_message = llm_run.outputs["generations"][0][0]["message"]
        tools = llm_run.extra.get("invocation_params", {}).get("tools", [])
        conversation = []

        for message in messages:
            if message["kwargs"]["type"] == "system":
                content = message["kwargs"]["content"]
                conversation.append({
                    "from": "system",
                    "value": self._format_system_message(content, tools)
                })

            elif message["kwargs"]["type"] == "ai":
                if message["kwargs"]["tool_calls"]:
                    tool_call = message["kwargs"]["tool_calls"][0]
                    conversation.append({
                        "from": "assistant",
                        "value": self._format_tool_call(tool_call)
                    })

                else:
                    content = message["kwargs"]["content"]
                    conversation.append({
                        "from": "assistant",
                        "value": content
                    })

            elif message["kwargs"]["type"] == "tool":
                tool_output = message["kwargs"]["content"]
                conversation.append({
                    "from": "ipython",
                    "value": tool_output
                })

            elif message["kwargs"]["type"] == "human":
                content = self._remove_prompt_sections(message["kwargs"]["content"])
                conversation.append({
                    "from": "user",
                    "value": content
                })

        if output_message:
            if output_message["kwargs"]["tool_calls"]:
                tool_call = output_message["kwargs"]["tool_calls"][0]
                conversation.append({
                    "from": "assistant",
                    "value": self._format_tool_call(tool_call)
                })
            else:
                content = output_message["kwargs"]["content"]
                conversation.append({
                    "from": "assistant",
                    "value": content
                })

        return conversation

    def get_all_conversations(self, after_date: datetime.datetime = None) -> dict:
        dataset_dict = {
            "conversations": []
        }

        llm_runs = self.get_llm_runs()
        for llm_run in llm_runs:
            if after_date is None or llm_run.end_time > after_date:
                conversation = self.get_sharegpt_format(llm_run)
                dataset_dict["conversations"].append(conversation)

        return dataset_dict