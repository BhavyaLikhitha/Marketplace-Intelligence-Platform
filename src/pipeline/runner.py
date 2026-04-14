"""Pipeline runner — executes blocks in sequence with audit logging."""

from __future__ import annotations

import logging
from typing import Callable

import pandas as pd

from src.registry.block_registry import BlockRegistry
from src.registry.function_registry import FunctionRegistry

logger = logging.getLogger(__name__)


class PipelineRunner:
    """
    Executes a sequence of transformation blocks on a DataFrame.

    The block_sequence may include a "__generated__" sentinel that gets
    replaced with agent-generated transform functions loaded from the registry.
    """

    def __init__(
        self,
        block_registry: BlockRegistry,
        function_registry: FunctionRegistry,
    ):
        self.block_registry = block_registry
        self.function_registry = function_registry

    def run(
        self,
        df: pd.DataFrame,
        block_sequence: list[str],
        generated_functions: list[dict] | None = None,
        column_mapping: dict[str, str] | None = None,
        config: dict | None = None,
    ) -> tuple[pd.DataFrame, list[dict]]:
        """
        Execute blocks in sequence.

        Args:
            df: Input DataFrame
            block_sequence: Ordered list of block names. "__generated__" is a sentinel.
            generated_functions: Functions from Agent 2 or registry hits.
            column_mapping: source_col -> unified_col rename mapping.
            config: Block configuration (DQ weights, domain, etc.)

        Returns:
            (result_df, audit_log)
        """
        config = config or {}
        generated_functions = generated_functions or []
        audit_log = []

        # Apply column mapping first (rename source columns to unified names)
        if column_mapping:
            # Build reverse mapping: target_col -> source_col for _apply_generated
            self._column_mapping = {v: k for k, v in column_mapping.items()}
            df = df.rename(columns=column_mapping)
            audit_log.append(
                {
                    "block": "column_mapping",
                    "rows_in": len(df),
                    "rows_out": len(df),
                    "columns_renamed": column_mapping,
                }
            )
        else:
            self._column_mapping = {}

        for block_name in block_sequence:
            rows_before = len(df)

            if block_name == "__generated__":
                df = self._apply_generated(df, generated_functions)
                audit_log.append(
                    {
                        "block": "__generated__",
                        "rows_in": rows_before,
                        "rows_out": len(df),
                        "functions_applied": len(generated_functions),
                    }
                )
            else:
                try:
                    block = self.block_registry.get(block_name)
                    df = block.run(df, config)
                    audit_log.append(block.audit_entry(rows_before, len(df)))
                    logger.info(
                        f"Block '{block_name}': {rows_before} -> {len(df)} rows"
                    )
                except KeyError:
                    logger.warning(f"Block '{block_name}' not found, skipping")
                    audit_log.append(
                        {
                            "block": block_name,
                            "rows_in": rows_before,
                            "rows_out": rows_before,
                            "error": "block not found",
                        }
                    )

        return df, audit_log

    def _apply_generated(
        self,
        df: pd.DataFrame,
        generated_functions: list[dict],
    ) -> pd.DataFrame:
        """Apply agent-generated or registry-loaded transform functions."""
        # Build reverse mapping: target_col -> source_col
        column_mapping = getattr(self, "_column_mapping", {})

        for func_info in generated_functions:
            fn_name = func_info["function_name"]
            file_path = func_info.get("file_path", "")
            registry_key = func_info.get("registry_key", "")

            try:
                fn: Callable = self.function_registry.load_function(file_path, fn_name)

                # Determine which column(s) the function operates on
                # Convention: function named transform_X operates on column X
                target_col = fn_name.replace("transform_", "")
                if target_col in df.columns:
                    # Column exists, apply transform directly
                    df[target_col] = df[target_col].apply(fn)
                else:
                    # Target column doesn't exist - check if there's a source column
                    source_col = column_mapping.get(target_col)
                    if source_col and source_col in df.columns:
                        # Apply transform to source column, rename to target
                        df[target_col] = df[source_col].apply(fn)
                        logger.info(
                            f"Applied {fn_name} to {source_col} -> {target_col}"
                        )
                    else:
                        # No source column available - create column with None
                        logger.warning(
                            f"No source column for {fn_name}, creating empty column"
                        )
                        df[target_col] = None

                if registry_key:
                    self.function_registry.increment_usage(registry_key)

                logger.info(f"Applied generated function: {fn_name}")
            except Exception as e:
                logger.error(f"Failed to apply generated function '{fn_name}': {e}")

        return df
