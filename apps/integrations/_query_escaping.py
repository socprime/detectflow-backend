"""String-escaping helpers for generated SIEM queries and deep-link state.

One home for the escaper family used by the View-in-SIEM providers, so escaping rules can't
drift between consoles that share a language: KQL is used by both Microsoft Sentinel and the
Kibana-family Discover links, rison by every Kibana-fork Discover state. Package-private —
imported only by ``apps/integrations/providers/*``.
"""

from __future__ import annotations


def _kql_string_literal(value: str) -> str:
    """KQL double-quoted string literal with backslashes and quotes escaped."""
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _spl_string_literal(value: str) -> str:
    """SPL double-quoted string literal with backslashes, quotes, and control chars escaped.

    A raw newline terminates an SPL double-quoted literal, so control characters are encoded
    with SPL's documented escapes (``\\n``, ``\\t``; ``\\r`` is normalized to ``\\n``) — a value
    with embedded control characters can't splice stray query text.
    """
    escaped = (
        value.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\r\n", "\\n")
        .replace("\n", "\\n")
        .replace("\r", "\\n")
        .replace("\t", "\\t")
    )
    return f'"{escaped}"'


def _rison_string(value: str) -> str:
    """Rison single-quoted string: ``!`` is the escape char (``!`` → ``!!``, ``'`` → ``!'``)."""
    escaped = value.replace("!", "!!").replace("'", "!'")
    return f"'{escaped}'"
