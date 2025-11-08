from typing import Dict, Any

def resolve_nested_key(data, key_path):
    """
    Safely resolve nested keys in a dictionary using dot notation.
    Example:
        resolve_nested_key({'a': {'b': 2}}, 'a.b') -> 2
    """
    if not key_path:
        return data

    keys = key_path.split(".")
    for key in keys:
        if isinstance(data, dict) and key in data:
            data = data[key]
        else:
            return None
    return data

