"""Block registry — discovers and loads pre-built transformation blocks."""

from __future__ import annotations

from src.blocks.base import Block
from src.blocks.strip_whitespace import StripWhitespaceBlock
from src.blocks.lowercase_brand import LowercaseBrandBlock
from src.blocks.remove_noise_words import RemoveNoiseWordsBlock
from src.blocks.strip_punctuation import StripPunctuationBlock
from src.blocks.extract_quantity_column import ExtractQuantityColumnBlock
from src.blocks.keep_quantity_in_name import KeepQuantityInNameBlock
from src.blocks.extract_allergens import ExtractAllergensBlock
from src.blocks.fuzzy_deduplicate import FuzzyDeduplicateBlock
from src.blocks.column_wise_merge import ColumnWiseMergeBlock
from src.blocks.golden_record_select import GoldenRecordSelectBlock
from src.blocks.dq_score import DQScorePreBlock, DQScorePostBlock
from src.blocks.llm_enrich import LLMEnrichBlock


# All registered block instances
_BLOCKS: dict[str, Block] = {
    "strip_whitespace": StripWhitespaceBlock(),
    "lowercase_brand": LowercaseBrandBlock(),
    "remove_noise_words": RemoveNoiseWordsBlock(),
    "strip_punctuation": StripPunctuationBlock(),
    "extract_quantity_column": ExtractQuantityColumnBlock(),
    "keep_quantity_in_name": KeepQuantityInNameBlock(),
    "extract_allergens": ExtractAllergensBlock(),
    "fuzzy_deduplicate": FuzzyDeduplicateBlock(),
    "column_wise_merge": ColumnWiseMergeBlock(),
    "golden_record_select": GoldenRecordSelectBlock(),
    "dq_score_pre": DQScorePreBlock(),
    "dq_score_post": DQScorePostBlock(),
    "llm_enrich": LLMEnrichBlock(),
}


class BlockRegistry:
    """Registry of pre-built transformation blocks."""

    def __init__(self) -> None:
        self.blocks = dict(_BLOCKS)

    def get(self, name: str) -> Block:
        """Get a block by name. Raises KeyError if not found."""
        if name not in self.blocks:
            raise KeyError(f"Block '{name}' not found. Available: {list(self.blocks.keys())}")
        return self.blocks[name]

    def list_blocks(self, domain: str | None = None) -> list[str]:
        """List available block names, optionally filtered by domain."""
        if domain is None:
            return list(self.blocks.keys())
        return [
            name for name, block in self.blocks.items()
            if block.domain in ("all", domain)
        ]

    def get_default_sequence(self, domain: str = "nutrition") -> list[str]:
        """
        Return the default block execution sequence for a domain.

        The __generated__ sentinel marks where agent-generated transforms are injected.
        """
        base = [
            "dq_score_pre",
            "__generated__",
            "strip_whitespace",
            "lowercase_brand",
            "remove_noise_words",
            "strip_punctuation",
        ]

        # Domain-specific block
        if domain == "pricing":
            base.append("keep_quantity_in_name")
        else:
            base.append("extract_quantity_column")

        if domain in ("nutrition", "safety"):
            base.append("extract_allergens")

        base.extend([
            "fuzzy_deduplicate",
            "column_wise_merge",
            "golden_record_select",
            "llm_enrich",
            "dq_score_post",
        ])
        return base
