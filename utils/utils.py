from sedarapi import SedarAPI
from consts import SEDAR_BASE_URL, SEDAR_EMAIL, SEDAR_PASSWORD
from cache.cacheable import CacheableRegistry
from typing import Callable
import json
import asyncio
import uuid
import inspect
import re

def get_sedar_default_workspace():
    sedar = SedarAPI(SEDAR_BASE_URL)
    sedar.connection.logger.setLevel("ERROR")

    return sedar.get_default_workspace()

def remove_json_code_block_markers(text: str):
    """
    Extracts JSON content from Markdown-style code blocks or from a raw JSON object or array.
    """
    # Match fenced code blocks with language specifier
    for lang in ("json", "sparql"):
        match = re.search(rf"```{lang}(.*?)```", text, re.DOTALL)
        if match:
            return match.group(1).strip()

    # Match fenced code block without language
    match = re.search(r"```(.*?)```", text, re.DOTALL)
    if match:
        return match.group(1).strip()

    # Match standalone JSON object or array at the end of the message
    match = re.search(r"(\{.*\}|\[.*\])\s*$", text, re.DOTALL)
    if match:
        return match.group(1).strip()

    return text.strip()

def load_json(text: str):
    """
    Parses the extracted JSON string into a Python object.
    """
    cleaned_text = remove_json_code_block_markers(text)
    return json.loads(cleaned_text)


def get_function_details(func: Callable) -> str:
    """
    Retrieve the name, signature, docstring, and source code of a function.

    Args:
        func: The function to analyze.

    Returns:
        A string representation of the function's details.
    """
    return inspect.getsource(func)

def generate_short_uuid(length: int = 8) -> str:
    return str(uuid.uuid4()).replace("-", "")[:length]

def get_minimal_docstring(docstring: str, sections_to_remove: list[str]) -> str:
    """Returns a more minimal docstring without the specified sections."""
    if docstring:
        # Remove each section and everything after it until the next section
        for section in sections_to_remove:
            docstring = re.sub(rf"({section}:.*?)(?=\s*(?=\w+:)|$)", "", docstring, flags=re.DOTALL)

        # Remove multiple blank lines
        docstring = re.sub(r"\n\s*\n", "\n", docstring)

    return docstring.strip() if docstring else ""

def is_async_context():
    try:
        asyncio.get_running_loop()
        return True
    except RuntimeError:
        return False