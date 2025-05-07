from utils.jupyter_helper import JupyterHelper
from consts import SEDAR_BASE_URL
from sedarapi import SedarAPI

class PromptCompressor:
    _sedar_api = None
    _username = None

    @classmethod
    def _initialize_sedar_api(cls):
        if cls._sedar_api is None:
            cls._sedar_api = SedarAPI(base_url=SEDAR_BASE_URL)
            cls._username = cls._sedar_api.login_gitlab().content["username"]

    def __init__(self):
        self._initialize_sedar_api()

    @classmethod
    def compress_prompt(cls, prompt: str) -> str:
        jupyter_helper = JupyterHelper(cls._username, cls._sedar_api.connection.jupyter_token)
        jupyter_helper.create_notebook(prompt_to_compress=prompt)
        output_messages = jupyter_helper.execute_notebook()
        return output_messages[-1]