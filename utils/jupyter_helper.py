import os
import sys
import json
import uuid
import datetime
import requests
import textwrap
import nbformat as nbf
from websocket import create_connection
from dotenv import load_dotenv

load_dotenv('../.env')

class JupyterHelper():

    def __init__(self, user_name, user_token):
        self.user_name = user_name
        self.user_token = user_token
        self.base_url = f'{os.environ["JUPYTERHUB_URL"]}/user/{user_name}/api'
        self.headers = {"Authorization": f"token {user_token}"}

    def _sanitize_prompt_text(self, prompt: str):
        return prompt.strip().replace('"""', "'''").replace("<", "[").replace("[llmlingua", "<llmlingua").replace("[/llmlingua", "</llmlingua")

    def _build_notebook_contents(self, prompt: str):
        prompt = self._sanitize_prompt_text(prompt)

        install_packages_code = textwrap.dedent('''
            !pip install llmlingua
        ''')

        compress_prompt_code = textwrap.dedent(f'''
            from llmlingua import PromptCompressor
            llm_lingua = PromptCompressor()
            prompt = """{prompt}"""
            compressed_prompt = llm_lingua.structured_compress_prompt(prompt, instruction="", question="")
            print(compressed_prompt['compressed_prompt'])
        ''')

        notebook = nbf.v4.new_notebook()
        notebook["cells"] = [
            nbf.v4.new_code_cell(install_packages_code),
            nbf.v4.new_code_cell(compress_prompt_code)
        ]

        return notebook
    
    def _send_execute_request(self, code):
        msg_type = "execute_request"
        content = {"code": code, "silent": False}
        hdr = {
            "msg_id": uuid.uuid1().hex,
            "username": "test",
            "session": uuid.uuid1().hex,
            "data": datetime.datetime.now().isoformat(),
            "msg_type": msg_type,
            "version": "5.0",
        }
        msg = {"header": hdr, "parent_header": hdr, "metadata": {}, "content": content}
        return msg
    
    def _start_kernel(self):
        url = f"{self.base_url}/kernels"
        try:
            response = requests.post(url, headers=self.headers)
            response.raise_for_status()
            return json.loads(response.text)
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Failed to start kernel: {e}")

    def _delete_kernel(self, kernel_id):
        url = f"{self.base_url}/kernels/{kernel_id}"
        try:
            response = requests.delete(url, headers=self.headers)
            response.raise_for_status()

            if response.text.strip():
                return json.loads(response.text)
            return None
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Failed to delete kernel: {e}")
        
    def _get_notebook_content(self, notebook_title):
        url = f"{self.base_url}/contents/{notebook_title}.ipynb"
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return json.loads(response.text)
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Failed to get notebook content: {e}")

    def create_notebook(self, prompt_to_compress: str, title="llmlingua"):
        notebook_source = self._build_notebook_contents(prompt_to_compress)

        nb_data = {
            "type": "notebook",
            "format": "json",
            "content": notebook_source,
            "name": title
        }

        response_put = requests.put(
            f'{self.base_url}/contents/{title}.ipynb',
            headers=self.headers,
            data=json.dumps(nb_data)
        )

        if response_put.status_code != 201 and response_put.status_code != 200:
            raise Exception(f"Error uploading notebook: {response_put.text}")

        return response_put.json(), 200

    def execute_notebook(self):
        kernel = self._start_kernel()
        notebook = self._get_notebook_content("llmlingua")

        code = [
            c["source"]
            for c in notebook["content"]["cells"]
            if len(c["source"]) > 0 and c["cell_type"] == "code"
        ]

        ws = create_connection(
            f"{self.base_url.replace('https', 'wss').replace('http', 'ws')}/kernels/{kernel['id']}/channels",
            header=self.headers
        )

        for c in code:
            ws.send(json.dumps(self._send_execute_request(c)))

        code_blocks_to_execute = len(code)
        output_messages = []

        while code_blocks_to_execute > 0:
            try:
                rsp = json.loads(ws.recv())
                msg_type = rsp["msg_type"]
                if msg_type == "error":
                    print({"exception": rsp["content"]}, sys.stderr)
                    raise Exception(rsp["content"]["traceback"][0])
            except Exception as _e:
                print(_e, sys.stderr)
                break

            if msg_type == "stream" and rsp["content"]["name"] == "stdout":
                output_messages.append(rsp["content"]["text"])

            if (
                msg_type == "execute_reply"
                and rsp["metadata"].get("status") == "ok"
                and rsp["metadata"].get("dependencies_met", False)
            ):
                code_blocks_to_execute -= 1

        ws.close()
        self._delete_kernel(kernel["id"])

        return output_messages
