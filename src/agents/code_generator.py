"""Agent 2 — Code Generator: LLM code generation + sandbox validation."""

from __future__ import annotations

import logging

from src.agents.state import PipelineState, GeneratedFunction
from src.agents.prompts import CODEGEN_PROMPT, CODEGEN_RETRY_PROMPT
from src.agents.sandbox import execute_in_sandbox
from src.models.llm import call_llm, get_codegen_llm
from src.registry.function_registry import FunctionRegistry

from pathlib import Path

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
REGISTRY_DIR = PROJECT_ROOT / "function_registry"


def _clean_code_response(raw: str) -> str:
    """Strip markdown fences from LLM code responses."""
    code = raw.strip()
    if code.startswith("```python"):
        code = code[len("```python"):].strip()
    elif code.startswith("```"):
        code = code[3:].strip()
    if code.endswith("```"):
        code = code[:-3].strip()
    return code


def generate_code_node(state: PipelineState) -> dict:
    """
    Agent 2: Generate transformation functions for each registry miss.

    Calls DeepSeek for code generation, validates in sandbox.
    """
    misses = state.get("registry_misses", [])
    retry_count = state.get("retry_count", 0)
    previous_functions = state.get("generated_functions", [])
    model = get_codegen_llm()

    generated: list[GeneratedFunction] = []

    # On retry, only re-generate functions that failed validation
    if retry_count > 0 and previous_functions:
        failed = [f for f in previous_functions if not f["validation_passed"]]
        # Rebuild misses from failed functions
        misses = [
            gap for gap in state.get("registry_misses", [])
            if any(
                f["function_name"] == f"transform_{gap['target_column']}"
                for f in failed
            )
        ]
        # Keep previously passed functions
        generated = [f for f in previous_functions if f["validation_passed"]]

    for gap in misses:
        target_col = gap["target_column"]
        target_type = gap.get("target_type", "string")
        source_col = gap.get("source_column")
        source_type = gap.get("source_type", "string")
        sample_values = gap.get("sample_values", [])
        function_name = f"transform_{target_col}"

        logger.info(f"Generating function: {function_name} (attempt {retry_count + 1})")

        # Check if this is a retry with previous code
        previous_code = None
        previous_error = None
        if retry_count > 0:
            prev = next(
                (f for f in previous_functions if f["function_name"] == function_name),
                None,
            )
            if prev:
                previous_code = prev.get("function_code", "")
                previous_error = prev.get("_validation_error", "Unknown error")

        if previous_code and previous_error:
            prompt = CODEGEN_RETRY_PROMPT.format(
                error=previous_error,
                previous_code=previous_code,
                target_column=target_col,
                target_type=target_type,
                sample_values=sample_values,
            )
        else:
            prompt = CODEGEN_PROMPT.format(
                target_column=target_col,
                target_type=target_type,
                source_column=source_col,
                source_type=source_type,
                sample_values=sample_values,
            )

        try:
            raw_code = call_llm(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
            )
            code = _clean_code_response(raw_code)

            # Validate in sandbox
            validation = execute_in_sandbox(
                function_code=code,
                function_name=function_name,
                sample_values=sample_values if sample_values else ["test", "", None],
                target_type=target_type,
            )

            registry_key = f"{source_type}_to_{target_type}_{target_col}"

            func: GeneratedFunction = {
                "function_name": function_name,
                "function_code": code,
                "file_path": "",  # Set after registration
                "registry_key": registry_key,
                "validation_passed": validation["passed"],
                "sample_outputs": validation.get("outputs", {}),
            }

            if not validation["passed"]:
                func["_validation_error"] = validation.get("error", "Unknown")
                logger.warning(
                    f"Validation failed for {function_name}: {validation.get('error')}"
                )
            else:
                logger.info(
                    f"Function {function_name} generated and validated successfully"
                )

            generated.append(func)

        except Exception as e:
            logger.error(f"Code generation failed for {target_col}: {e}")
            generated.append({
                "function_name": function_name,
                "function_code": "",
                "file_path": "",
                "registry_key": "",
                "validation_passed": False,
                "sample_outputs": {},
                "_validation_error": str(e),
            })

    return {
        "generated_functions": generated,
        "retry_count": retry_count + 1,
    }


def validate_code_node(state: PipelineState) -> dict:
    """Check if all generated functions passed validation. (Pass-through node for routing.)"""
    # This node exists for the conditional edge to inspect state
    return {}


def register_functions_node(state: PipelineState) -> dict:
    """Save validated functions to the persistent function registry."""
    generated = state.get("generated_functions", [])
    registry = FunctionRegistry(REGISTRY_DIR)
    misses = state.get("registry_misses", [])

    updated_functions = []
    for func in generated:
        if not func["validation_passed"]:
            updated_functions.append(func)
            continue

        # Find the gap metadata for this function
        target_col = func["function_name"].replace("transform_", "")
        gap = next(
            (g for g in misses if g["target_column"] == target_col),
            None,
        )

        metadata = {
            "domain": state.get("domain", "unknown"),
            "source_type": gap.get("source_type", "string") if gap else "string",
            "target_type": gap.get("target_type", "string") if gap else "string",
            "tags": [target_col],
            "validation_passed": True,
        }

        file_path = registry.save(
            key=func["registry_key"],
            function_name=func["function_name"],
            function_code=func["function_code"],
            metadata=metadata,
        )

        func_copy = dict(func)
        func_copy["file_path"] = file_path
        updated_functions.append(func_copy)
        logger.info(f"Registered function: {func['function_name']} -> {file_path}")

    return {"generated_functions": updated_functions}
