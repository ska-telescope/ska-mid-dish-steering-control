"""Common utility functions for private use inside the SCU package."""


import json
import logging
from pathlib import Path
from typing import Any

from .constants import JSONData

logger = logging.getLogger("ska-mid-ds-scu")


def load_json_file(file_path: Path) -> dict[str, JSONData] | None:
    """
    Load JSON file.

    :param file_path: Path of JSON file to load.
    :return: Decoded JSON file contents as nested dictionary, or None if it failed.
    """
    try:
        with open(file_path, "r", encoding="UTF-8") as file:
            try:
                return json.load(file)
            except json.JSONDecodeError:
                logger.error(f"The file '{file_path}' is not valid JSON.")
    except (UnicodeDecodeError, OSError) as e:
        logger.error(f"Caught exception trying to read file '{file_path}': {e}")
    return None


def check_float(value: Any) -> Any:
    """Try casting a value to a float, otherwise return it unchanged."""
    try:
        return float(value)
    except (ValueError, TypeError):
        return value
