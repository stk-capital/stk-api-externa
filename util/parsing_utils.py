import re
import json
from typing import Dict, Any
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_brace_arguments(text: str) -> Dict[str, Any]:
    """
    Extract key-value pairs from text enclosed in double braces.
    """
    pattern = r"\{\{(.*?)\}\}"
    matches = re.findall(pattern, text, re.DOTALL)
    extracted = {}
    for match in matches:
        try:
            key, value = match.split(":", 1)
            key = key.strip()
            value = value.strip()
            if value.startswith("[") or value.startswith("{"):
                value = value.replace("\n", "").replace("\r", "").strip()
                try:
                    extracted[key] = json.loads(value)
                except json.JSONDecodeError:
                    extracted[key] = value
            else:
                extracted[key] = value
        except ValueError:
            continue
    return extracted



def extract_json_from_content(content: str) -> str:
    """
    Extract JSON string from content. If the content is already a valid JSON string,
    return it as it is, otherwise look for content delimited by ```json and ```.
    """
    try:
        # Try to directly parse the content to check if it's valid JSON.
        json.loads(content)
        return content
    except json.JSONDecodeError:
        pass

    pattern = r"```json\s*(.*?)\s*```"
    match = re.search(pattern, content, re.DOTALL)
    if match:
        json_str = match.group(1)
        return json_str.replace('\\n', '\n')
    else:
        raise ValueError("JSON content not found.")


# Auxiliary functions
def extract_brace_arguments_langgraph(text):
    # Regular expression to match {{key:value}} pairs
    # text = '''The screen is still black. It might be in sleep mode or powered off. Let's try pressing a key to wake it up.{{route:Desktop Hotkey}}{{keys:["space"]}}'''
    # print("text:"+text)
    pattern = r"\{\{(.*?)\}\}"
    matches = re.findall(pattern, str(text))

    # Dictionary to store extracted key-value pairs
    extracted_dict = {}

    for match in matches:
        try:
            # Split the match into key and value at the first colon
            key, value = match.split(':', 1)
            extracted_dict[key.strip()] = value.strip()
        except ValueError:
            # Handle cases where there is no colon in the match
            continue

    return extracted_dict