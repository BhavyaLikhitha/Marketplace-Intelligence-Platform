"""Persistent function registry — stores LLM-generated transformation functions."""

from __future__ import annotations

import importlib.util
import json
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional


class FunctionRegistry:
    """
    Registry for agent-generated transformation functions.

    Stores:
      - registry.json: index of all saved functions
      - functions/*.py: the actual Python files
    """

    def __init__(self, registry_dir: str | Path = "function_registry"):
        self.registry_dir = Path(registry_dir)
        self.index_path = self.registry_dir / "registry.json"
        self.functions_dir = self.registry_dir / "functions"
        self.functions_dir.mkdir(parents=True, exist_ok=True)
        self._ensure_index()

    def _ensure_index(self) -> None:
        if not self.index_path.exists():
            self.index_path.write_text("[]")

    def _load_index(self) -> list[dict]:
        return json.loads(self.index_path.read_text())

    def _save_index(self, entries: list[dict]) -> None:
        self.index_path.write_text(json.dumps(entries, indent=2))

    def lookup(
        self,
        source_type: str,
        target_type: str,
        tags: list[str] | None = None,
    ) -> Optional[dict]:
        """
        Find a registered function matching the type signature.

        Matches on source_type + target_type first, then ranks by tag overlap.
        Returns the best registry entry or None.
        """
        entries = self._load_index()
        candidates = [
            e for e in entries
            if e["source_type"] == source_type and e["target_type"] == target_type
        ]
        if not candidates:
            return None
        if not tags:
            return candidates[0]

        # Rank by tag overlap
        def tag_score(entry: dict) -> int:
            return len(set(entry.get("tags", [])) & set(tags))

        candidates.sort(key=tag_score, reverse=True)
        return candidates[0]

    def save(
        self,
        key: str,
        function_name: str,
        function_code: str,
        metadata: dict,
    ) -> str:
        """
        Save a function to a .py file and add an entry to registry.json.

        Returns the file path relative to registry_dir.
        """
        file_name = f"{key}.py"
        file_path = self.functions_dir / file_name
        file_path.write_text(function_code)

        entries = self._load_index()

        # Update existing or append new
        existing = next((e for e in entries if e["key"] == key), None)
        entry = {
            "key": key,
            "function_name": function_name,
            "file": f"functions/{file_name}",
            "created_for_domain": metadata.get("domain", "unknown"),
            "source_type": metadata.get("source_type", "string"),
            "target_type": metadata.get("target_type", "string"),
            "tags": metadata.get("tags", []),
            "used_count": 0,
            "last_used": None,
            "validation_passed": metadata.get("validation_passed", True),
            "created_at": datetime.now().isoformat(),
        }
        if existing:
            idx = entries.index(existing)
            entry["used_count"] = existing["used_count"]
            entries[idx] = entry
        else:
            entries.append(entry)

        self._save_index(entries)
        return str(file_path)

    def load_function(self, file_path: str, function_name: str) -> Callable:
        """Load a saved function via importlib."""
        path = Path(file_path)
        if not path.is_absolute():
            path = self.registry_dir / path

        spec = importlib.util.spec_from_file_location(function_name, str(path))
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return getattr(module, function_name)

    def increment_usage(self, key: str) -> None:
        """Bump used_count and update last_used timestamp."""
        entries = self._load_index()
        for entry in entries:
            if entry["key"] == key:
                entry["used_count"] += 1
                entry["last_used"] = datetime.now().isoformat()
                break
        self._save_index(entries)

    def list_all(self) -> list[dict]:
        """Return all registry entries."""
        return self._load_index()
