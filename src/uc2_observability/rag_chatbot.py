"""
UC2 Observability Layer — RAG Chatbot

Planned implementation:
- ChromaDB vector store over audit logs, anomaly reports, pipeline run history
- Claude-powered question answering
- Example queries:
  - "Why did the March 28th run produce fewer rows?"
  - "Which block caused the null brand_owner spike?"
  - "How many products were quarantined and why?"

Dependencies: chromadb, langchain, anthropic
"""

from __future__ import annotations


class ObservabilityChatbot:
    """RAG chatbot for pipeline observability queries."""

    def __init__(self, chroma_collection: str = "pipeline_audit"):
        raise NotImplementedError("UC2 — planned for next sprint")

    def ingest_audit_logs(self, audit_log_dir: str) -> int:
        """
        Ingest audit logs into ChromaDB.

        Returns number of documents indexed.
        """
        raise NotImplementedError

    def query(self, question: str) -> str:
        """
        Answer a natural language question about pipeline runs.

        Uses RAG: retrieve relevant audit log chunks, feed to Claude.
        """
        raise NotImplementedError

    def get_relevant_context(self, question: str, top_k: int = 5) -> list[str]:
        """Retrieve top-k relevant audit log chunks from ChromaDB."""
        raise NotImplementedError
