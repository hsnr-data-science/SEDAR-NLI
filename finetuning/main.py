import json
import datetime
from datasets import Dataset
from dotenv import load_dotenv
from langsmith.client import Client
from finetuning.langsmith_chat_loader import LangSmithChatLoader

load_dotenv()
# client = Client()
# project_name = "sedar-langgraph-agent-finetune-1"

# chat_loader = LangSmithChatLoader(project_name, client)

# dataset = chat_loader.get_all_conversations(after_date=datetime.datetime.now() - datetime.timedelta(minutes=10))

# # write the dataset json to a file
# with open("dataset.json", "w") as f:
#    f.write(json.dumps(dataset, indent=4))



# # load the dataset from the file
# with open("dataset.json", "r") as f:
#    dataset = json.load(f)

# print(len(dataset["conversations"]))

with open("./finetuning/dataset copy.jsonl", "r") as f:
   lines = f.readlines()

conversations = []

for line in lines:
   line_json = json.loads(line.strip())

   for i, message in enumerate(line_json):

      if message["role"] == "assistant":
         line_content = message["content"]

         # check if line_content is json string
         if line_content.startswith("{") and line_content.endswith("}"):
            try:
               line_content_json = json.loads(line_content)
               if "name" in line_content_json and "parameters" in line_content_json:
                  # replace "parameters" with "arguments"
                  line_content_json["arguments"] = line_content_json.pop("parameters")
                  line_json[i]["content"] = f"<tool_call>{json.dumps(line_content_json)}</tool_call>"
            except:
               pass
      elif message["role"] == "system":
         line_content = message["content"]

         current_prompt = "You have access to the following functions. To call a function, please respond with JSON for a function call.\nRespond in the format {\"name\": function name, \"parameters\": dictionary of argument name and its value}."

         if current_prompt in line_content:
            line_json[i]["content"] = line_json[i]["content"].replace(current_prompt, "You have access to the following functions. To call a function, please respond with a JSON object with function name and arguments within <tool_call></tool_call> XML tags:\n<tool_call>\n{\"name\": <function-name>, \"arguments\": dictionary of argument name and its value}\n</tool_call>")

   conversations.append(line_json)

with open("./finetuning/dataset_modified.jsonl", "w") as f:
    for conversation in conversations:
        f.write(json.dumps(conversation) + "\n")

dataset_dict = {
   "conversations": conversations
}

dataset = Dataset.from_dict(dataset_dict)

print(dataset)


















# from datasets import Dataset


# test = [
#     {
#        "from":"system",
#        "value":"You are a helpful assistant with access to the following functions. Use them if required -\n{\n \"name\": \"send_email\",\n \"description\": \"Send an email to a recipient\",\n \"parameters\": {\n \"type\": \"object\",\n \"properties\": {\n \"recipient\": {\n \"type\": \"string\",\n \"description\": \"The email address of the recipient\"\n },\n \"subject\": {\n \"type\": \"string\",\n \"description\": \"The subject of the email\"\n },\n \"message\": {\n \"type\": \"string\",\n \"description\": \"The body of the email\"\n }\n },\n \"required\": [\n \"recipient\",\n \"subject\",\n \"message\"\n ]\n }\n}\n"
#     },
#     {
#        "from":"human",
#        "value":"I need to send an email to my boss. Can you help me with that?"
#     },
#     {
#        "from":"gpt",
#        "value":"Of course, I can help you with that. Could you please provide me with the recipient's email address, the subject of the email, and the message you want to send? <|endoftext|>"
#     },
#     {
#        "from":"human",
#        "value":"Sure, the email address is boss@company.com. The subject is \"Project Update\" and the message is \"Dear Boss, I have completed the project as per the deadline. Please review it at your earliest convenience. Regards, [User's Name]\"."
#     },
#     {
#        "from":"gpt",
#        "value":"<functioncall> {\"name\": \"send_email\", \"arguments\": '{\n \"recipient\": \"boss@company.com\",\n \"subject\": \"Project Update\",\n \"message\": \"Dear Boss, I have completed the project as per the deadline. Please review it at your earliest convenience. Regards, [User's Name]\"\n}'} <|endoftext|>"
#     },
#     {
#        "from":"tool",
#        "value":"{\"status\": \"success\", \"message\": \"Email has been sent successfully\"}"
#     },
#     {
#        "from":"gpt",
#        "value":"The email has been sent successfully to your boss. Is there anything else you need help with? <|endoftext|>"
#     }
# ]

# dataset_dict = {
#     "conversations": [
#         test
#     ]
# }



# dataset = Dataset.from_dict(dataset_dict)



# print(dataset)
