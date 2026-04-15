"""DynamicMappingBlock — declarative YAML-driven column operations.

Replaces LLM-generated Python blocks for simple operations (set_null,
type_cast, set_default, format_transform). Operations are defined in a
YAML file and executed deterministically with correct null handling.
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from src.blocks.base import Block
from src.blocks.mapping_io import read_mapping_yaml

logger = logging.getLogger(__name__)

# Pandas nullable dtype mapping for set_null operations.
# Using nullable dtypes ensures proper NA semantics (no "None" strings).
_NULL_DTYPE_MAP = {
    "float": "Float64",
    "integer": "Int64",
    "int": "Int64",
    "boolean": "boolean",
    "bool": "boolean",
    "string": "string",
    "str": "string",
}


class DynamicMappingBlock(Block):
    """Declarative YAML-driven column operations.

    Reads a YAML mapping file and applies each operation in sequence.
    Supports: set_null, set_default, type_cast, format_transform.

    The block name starts with ``DYNAMIC_MAPPING_`` so that
    ``PipelineRunner._expand_sequence`` picks it up via prefix matching
    when expanding the ``__generated__`` sentinel.
    """

    def __init__(self, domain: str, yaml_path: str) -> None:
        self._yaml_path = yaml_path
        self._operations = read_mapping_yaml(yaml_path)
        self.name = f"DYNAMIC_MAPPING_{domain}"
        self.domain = domain
        self.description = f"Declarative column operations from {yaml_path}"
        self.inputs = [
            op["source"]
            for op in self._operations
            if op.get("source")
        ]
        self.outputs = [op["target"] for op in self._operations]

    @property
    def operations(self) -> list[dict[str, Any]]:
        return list(self._operations)

    def run(self, df: pd.DataFrame, config: dict | None = None) -> pd.DataFrame:
        df = df.copy()
        for op in self._operations:
            action = op["action"]
            handler = _ACTION_HANDLERS.get(action)
            if handler is None:
                logger.warning(f"Unknown action '{action}' for target '{op['target']}' — skipping")
                continue
            df = handler(df, op)
        return df


# ── Action handlers ──────────────────────────────────────────────────


def _handle_set_null(df: pd.DataFrame, op: dict) -> pd.DataFrame:
    """Create a column filled with proper typed null values."""
    target = op["target"]
    col_type = op.get("type", "string")
    dtype = _NULL_DTYPE_MAP.get(col_type, "string")
    df[target] = pd.array([pd.NA] * len(df), dtype=dtype)
    logger.debug(f"set_null: created '{target}' as {dtype} (all NA)")
    return df


def _handle_set_default(df: pd.DataFrame, op: dict) -> pd.DataFrame:
    """Create a column with a user-specified default value."""
    target = op["target"]
    col_type = op.get("type", "string")
    default = op.get("default_value")

    if default is None:
        return _handle_set_null(df, op)

    value = _cast_value(default, col_type)
    dtype = _NULL_DTYPE_MAP.get(col_type, "string")
    df[target] = pd.array([value] * len(df), dtype=dtype)
    logger.debug(f"set_default: created '{target}' = {value!r}")
    return df


def _handle_type_cast(df: pd.DataFrame, op: dict) -> pd.DataFrame:
    """Convert an existing source column to the target type."""
    source = op["source"]
    target = op["target"]
    col_type = op.get("type", "string")

    if source not in df.columns:
        logger.warning(f"type_cast: source column '{source}' not found — falling back to set_null")
        return _handle_set_null(df, op)

    if col_type in ("float", "integer", "int"):
        df[target] = pd.to_numeric(df[source], errors="coerce")
        if col_type in ("integer", "int"):
            df[target] = df[target].astype("Int64")
    elif col_type in ("string", "str"):
        # Convert to string without producing literal "None" / "nan"
        df[target] = df[source].astype("string")
    elif col_type in ("boolean", "bool"):
        df[target] = (
            df[source]
            .astype(str)
            .str.strip()
            .str.lower()
            .map({"true": True, "1": True, "yes": True,
                   "false": False, "0": False, "no": False})
            .astype("boolean")
        )
    else:
        df[target] = df[source].astype("string")

    # If source and target differ, drop the original source column
    if source != target and source in df.columns:
        df = df.drop(columns=[source])

    logger.debug(f"type_cast: '{source}' → '{target}' as {col_type}")
    return df


def _handle_format_transform(df: pd.DataFrame, op: dict) -> pd.DataFrame:
    """Apply a named format transformation."""
    source = op.get("source", op["target"])
    target = op["target"]
    transform = op.get("transform", "to_string")

    if source not in df.columns:
        logger.warning(f"format_transform: source '{source}' not found — falling back to set_null")
        return _handle_set_null(df, op)

    if transform == "to_string":
        df[target] = df[source].astype("string")
    elif transform == "parse_date":
        df[target] = pd.to_datetime(df[source], errors="coerce")
    elif transform == "to_lowercase":
        df[target] = df[source].astype("string").str.lower()
    else:
        logger.warning(f"Unknown transform '{transform}' — copying column as-is")
        df[target] = df[source]

    if source != target and source in df.columns:
        df = df.drop(columns=[source])

    logger.debug(f"format_transform({transform}): '{source}' → '{target}'")
    return df


def _cast_value(value: Any, col_type: str) -> Any:
    """Cast a raw default value to the appropriate Python type."""
    if col_type in ("float",):
        return float(value)
    if col_type in ("integer", "int"):
        return int(value)
    if col_type in ("boolean", "bool"):
        return str(value).lower() in ("true", "1", "yes")
    return str(value)


_ACTION_HANDLERS = {
    "set_null": _handle_set_null,
    "set_default": _handle_set_default,
    "type_cast": _handle_type_cast,
    "format_transform": _handle_format_transform,
}
