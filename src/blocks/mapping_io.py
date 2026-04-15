"""YAML I/O utilities for declarative column mapping files."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

_GENERATED_DIR = Path(__file__).resolve().parent / "generated"

VALID_ACTIONS = {"set_null", "set_default", "type_cast", "format_transform"}
REQUIRED_FIELDS = {"target", "type", "action"}


def write_mapping_yaml(
    domain: str,
    dataset_name: str,
    operations: list[dict[str, Any]],
) -> Path:
    """Write column operations to a YAML mapping file.

    Args:
        domain: Pipeline domain (e.g., "nutrition").
        dataset_name: Source dataset stem (e.g., "usda_sample_raw").
        operations: List of operation dicts, each with at least
            {target, type, action} and optionally {source, source_type,
            status, reason, default_value, transform}.

    Returns:
        Path to the written YAML file.
    """
    domain_dir = _GENERATED_DIR / domain
    domain_dir.mkdir(parents=True, exist_ok=True)

    file_path = domain_dir / f"DYNAMIC_MAPPING_{dataset_name}.yaml"
    data = {"column_operations": operations}
    file_path.write_text(yaml.dump(data, default_flow_style=False, sort_keys=False))
    logger.info(f"Wrote mapping YAML: {file_path} ({len(operations)} operations)")
    return file_path


def read_mapping_yaml(yaml_path: str | Path) -> list[dict[str, Any]]:
    """Read and validate column operations from a YAML mapping file.

    Returns:
        List of validated operation dicts.

    Raises:
        ValueError: If the YAML is malformed or contains invalid operations.
    """
    path = Path(yaml_path)
    if not path.exists():
        raise FileNotFoundError(f"Mapping YAML not found: {path}")

    data = yaml.safe_load(path.read_text())
    if not isinstance(data, dict) or "column_operations" not in data:
        raise ValueError(f"Invalid mapping YAML: missing 'column_operations' key in {path}")

    operations = data["column_operations"]
    if not isinstance(operations, list):
        raise ValueError(f"Invalid mapping YAML: 'column_operations' must be a list in {path}")

    for i, op in enumerate(operations):
        missing = REQUIRED_FIELDS - set(op.keys())
        if missing:
            raise ValueError(
                f"Operation {i} in {path} missing required fields: {missing}"
            )
        if op["action"] not in VALID_ACTIONS:
            raise ValueError(
                f"Operation {i} in {path} has invalid action '{op['action']}'. "
                f"Valid actions: {VALID_ACTIONS}"
            )
        if op["action"] == "type_cast" and "source" not in op:
            raise ValueError(
                f"Operation {i} in {path}: type_cast requires a 'source' field"
            )

    return operations


def merge_hitl_decisions(
    operations: list[dict[str, Any]],
    decisions: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    """Apply HITL decisions to column operations.

    Args:
        operations: List of operation dicts (typically from check_registry_node).
        decisions: Dict mapping target_column to decision dict.
            Each decision has {action: "accept_null"|"exclude"|"set_default",
            value?: <default_value>}.

    Returns:
        Updated operations list with HITL decisions applied.
        "exclude" columns still get set_null (column is created but not required).
    """
    updated = []
    for op in operations:
        target = op["target"]
        decision = decisions.get(target)
        if decision is None:
            updated.append(op)
            continue

        if decision.get("action") == "set_default":
            op = dict(op)
            op["action"] = "set_default"
            op["default_value"] = decision["value"]
            op.pop("status", None)
            updated.append(op)
        elif decision.get("action") in ("accept_null", "exclude"):
            # Both keep the set_null op — "exclude" additionally patches
            # the unified schema (handled in check_registry_node).
            updated.append(op)

    return updated
