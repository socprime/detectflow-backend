import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import yaml
from pydantic import TypeAdapter, ValidationError

HOSTNAME_FIELDS_KEY = "_detect_flow_hostname_fields"
TIMESTAMP_FIELDS_KEY = "_detect_flow_timestamp_fields"

ROOT_FOLDER_ORDER = {
    "_product": 0,
    "_category": 1,
    "_service": 2,
}

CHILD_FOLDER_ORDER = {
    "_category": 0,
    "_service": 1,
}
UUID_PATTERN = re.compile(r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b")
PEM_CERT_BLOCK_PATTERN = re.compile(r"-----BEGIN CERTIFICATE-----(.*?)-----END CERTIFICATE-----", re.DOTALL)
PEM_LINE_LENGTH = 64
# YAML plain scalars cannot start with ``@`` (reserved indicator). LLMs often emit unquoted
# Elastic ``@timestamp``, which then fails ``yaml.safe_load`` with ScannerError. Both sides of a
# mapping entry are repaired: the value (``Timestamp: @timestamp``, ``- @timestamp``) and the key
# (``@timestamp: event.created``). Trailing comments are preserved rather than blocking the match.
_UNQUOTED_AT_YAML_VALUE = re.compile(
    r"^(?P<prefix>\s*(?:-\s+|[^:#\n]+:\s+))(?P<value>@[^\s#'\"]+)(?P<tail>[^\S\n]*(?:#.*)?)$",
    re.MULTILINE,
)
_UNQUOTED_AT_YAML_KEY = re.compile(
    r"^(?P<indent>\s*(?:-\s+)?)(?P<key>@[^\s#'\":]+)(?P<sep>:(?:\s|$))",
    re.MULTILINE,
)


@dataclass(frozen=True)
class MappingMetadata:
    hostname_fields: list[str]
    timestamp_fields: list[str]


def coerce_config_bool(value: Any, *, default: bool) -> bool:
    """Coerce a JSONB integration-config value to bool, tolerating stringly-typed legacy data.

    Config dicts (e.g. GitLab integration ``verify_ssl``/``auto_pull``) are validated against a
    Pydantic schema on write, but code reading them back directly must not do a plain ``bool()``
    cast — that is truthy for any non-empty string, so a stored ``"false"`` would incorrectly
    evaluate as enabled. Uses Pydantic's lax bool parsing (accepts "true"/"false"/"1"/"0"/etc.,
    case-insensitive) instead.
    """
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    try:
        return TypeAdapter(bool).validate_python(value)
    except ValidationError:
        return default


def normalize_pem_bundle(value: str) -> str:
    """Reformat a CA certificate bundle into strict PEM (proper newlines, 64-char body lines).

    Tolerates a certificate bundle whose original newlines were flattened into spaces or lost
    entirely — a real-world failure mode when a multi-certificate chain is pasted into a
    single-line form field. OpenSSL's PEM parser requires the ``-----BEGIN/END CERTIFICATE-----``
    markers to each be on their own line, so a flattened bundle fails to load
    (``no start line: cadata does not contain a certificate``) even though the certificate bytes
    themselves are intact. Non-PEM text between blocks (e.g. ``subject=``/``issuer=`` annotations
    from ``openssl s_client -showcerts`` output) is dropped. Returns the input unchanged if no
    ``CERTIFICATE`` block is found (e.g. an empty string, or genuinely invalid input — left for
    the caller to reject).
    """
    blocks = PEM_CERT_BLOCK_PATTERN.findall(value)
    if not blocks:
        return value
    rebuilt = []
    for raw_body in blocks:
        b64 = re.sub(r"\s+", "", raw_body)
        wrapped = "\n".join(b64[i : i + PEM_LINE_LENGTH] for i in range(0, len(b64), PEM_LINE_LENGTH))
        rebuilt.append(f"-----BEGIN CERTIFICATE-----\n{wrapped}\n-----END CERTIFICATE-----")
    return "\n".join(rebuilt) + "\n"


def strip_nonempty_strings(values: list[str]) -> list[str]:
    """Strip entries and drop empties, preserving order and duplicates.

    Shared normalization for operator-entered string lists interpolated into generated
    queries (e.g. the Sigma rule ids in every View-in-SIEM query renderer).
    """
    return [value.strip() for value in values if value and value.strip()]


def _normalize_string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        candidates = [value]
    elif isinstance(value, list):
        candidates = value
    else:
        return []

    normalized: list[str] = []
    seen: set[str] = set()
    for item in candidates:
        if not isinstance(item, str):
            continue
        stripped = item.strip()
        if not stripped or stripped in seen:
            continue
        seen.add(stripped)
        normalized.append(stripped)
    return normalized


def quote_yaml_unsafe_field_scalars(mapping: str) -> str:
    """Quote unquoted ``@…`` field names so the mapping is valid YAML.

    Example: ``- @timestamp`` → ``- "@timestamp"``. Handles both mapping keys and values, keeps any
    trailing comment, and leaves already-quoted values unchanged. Only a *leading* ``@`` is repaired,
    so ``user@example.com`` and any ``@`` inside a comment are untouched. Idempotent — callers
    re-apply it to stored mappings that may already have been repaired.

    Out of scope (left invalid for the caller to reject): flow style (``[@timestamp]``) and non-``@``
    reserved indicators. Line-based, so a lone ``@`` token inside a literal block scalar would be
    rewritten — field-mapping YAML has no block scalars.
    """
    if not mapping or "@" not in mapping:
        return mapping
    repaired = _UNQUOTED_AT_YAML_KEY.sub(r'\g<indent>"\g<key>"\g<sep>', mapping)
    return _UNQUOTED_AT_YAML_VALUE.sub(r'\g<prefix>"\g<value>"\g<tail>', repaired)


def normalize_field_mapping_yaml(mapping: str | None) -> str | None:
    """Quote unsafe scalars and validate Sigma→event field-mapping YAML.

    Accepts ``None`` / blank unchanged. Otherwise quotes unquoted ``@…`` field names,
    requires a YAML mapping whose values are ``str``, ``list[str]``, or ``null``
    (including reserved ``_detect_flow_*`` metadata keys), and returns the repaired
    string for persistence.

    Raises:
        ValueError: If the YAML is invalid or not a field-mapping dictionary.
    """
    if mapping is None or not mapping.strip():
        return mapping

    repaired = quote_yaml_unsafe_field_scalars(mapping)
    try:
        payload = yaml.safe_load(repaired)
    except yaml.YAMLError as e:
        raise ValueError(f"Invalid mapping YAML: {e}") from e

    if payload is None:
        return repaired

    if not isinstance(payload, dict):
        raise ValueError("Mapping must be a YAML dictionary of sigma fields to event fields")

    for key, value in payload.items():
        if not isinstance(key, str):
            raise ValueError("Mapping keys must be strings")
        if value is None or isinstance(value, str):
            continue
        if isinstance(value, list):
            if not all(isinstance(item, str) for item in value):
                raise ValueError(f"Invalid field mapping for key {key}: list items must be strings")
            continue
        raise ValueError(f"Invalid field mapping for key {key}: value must be a string, list of strings, or null")

    return repaired


def parse_mapping_metadata(mapping: str | None) -> MappingMetadata:
    """Extract Detect Flow metadata fields embedded in mapping YAML."""
    if not mapping:
        return MappingMetadata(hostname_fields=[], timestamp_fields=[])

    try:
        payload = yaml.safe_load(quote_yaml_unsafe_field_scalars(mapping))
    except yaml.YAMLError:
        return MappingMetadata(hostname_fields=[], timestamp_fields=[])

    if not isinstance(payload, dict):
        return MappingMetadata(hostname_fields=[], timestamp_fields=[])

    return MappingMetadata(
        hostname_fields=_normalize_string_list(payload.get(HOSTNAME_FIELDS_KEY)),
        timestamp_fields=_normalize_string_list(payload.get(TIMESTAMP_FIELDS_KEY)),
    )


def strip_mapping_metadata(mapping: str | None) -> str | None:
    """Remove Detect Flow metadata keys from mapping YAML before runtime use."""
    if not mapping:
        return mapping

    try:
        payload = yaml.safe_load(quote_yaml_unsafe_field_scalars(mapping))
    except yaml.YAMLError:
        return mapping

    if not isinstance(payload, dict):
        return mapping

    payload.pop(HOSTNAME_FIELDS_KEY, None)
    payload.pop(TIMESTAMP_FIELDS_KEY, None)
    return yaml.safe_dump(payload, sort_keys=False, allow_unicode=True).strip()


def normalize_sigma_uuid(value: object) -> str | None:
    """Return a canonical UUID string for Sigma ids that are valid UUIDs."""
    if not isinstance(value, str):
        return None
    try:
        return str(UUID(value))
    except ValueError:
        return None


def get_sigma_uuid_from_sigma_body(sigma_text: str | None) -> str | None:
    if sigma_text is None:
        return None
    try:
        sigma_dict = yaml.safe_load(sigma_text)
    except yaml.YAMLError:
        return None
    if not isinstance(sigma_dict, dict):
        return None
    return normalize_sigma_uuid(sigma_dict.get("id"))


SIGMA_LEVEL_MAX_LENGTH = 255


def normalize_sigma_level(value: object) -> str | None:
    """Return the canonical lowercase Sigma ``level``, or ``None`` when absent/unusable.

    Sigma levels are informational/low/medium/high/critical, but bodies in the wild also carry
    non-standard values, so nothing is rejected here beyond blanks — the value is only trimmed
    and lowercased. Callers persist it as a plain string (``rules.level``), never an enum.

    Truncated to the column width: a malformed body can put arbitrary prose behind ``level:``,
    and PostgreSQL raises 22001 on overflow rather than truncating — which would abort a whole
    sync batch or the Alembic backfill, not just the offending rule.
    """
    if not isinstance(value, str):
        return None
    return value.strip().lower()[:SIGMA_LEVEL_MAX_LENGTH] or None


def get_level_from_sigma_body(sigma_text: str | None) -> str | None:
    """Return the top-level Sigma ``level`` declared in a rule or high-order Sigma body.

    Parsing goes through YAML rather than a regex on purpose: real bodies quote the value
    (``level: "high"``), use CRLF line endings, and occasionally carry an indented ``Level:``
    inside ``detection``/``logsource`` that must NOT be picked up as the rule's severity.
    """
    if sigma_text is None:
        return None
    try:
        sigma_dict = yaml.safe_load(sigma_text)
    except yaml.YAMLError:
        return None
    if not isinstance(sigma_dict, dict):
        return None
    return normalize_sigma_level(sigma_dict.get("level"))


def get_fields_from_sigma(sigma_text: str) -> set[str]:
    try:
        sigma_dict = yaml.safe_load(sigma_text)
    except yaml.YAMLError:
        return set()
    if not isinstance(sigma_dict, dict):
        return set()
    detection = sigma_dict.get("detection")
    if not isinstance(detection, dict):
        return set()

    fields = set()
    for _, statements in detection.items():
        if not isinstance(statements, list):
            statements = [statements]
        for statement in statements:
            if not isinstance(statement, dict):
                continue
            for field in statement.keys():
                field = field.split("|")[0]
                fields.add(field)
    return fields


def build_rule_folder_tree(folder_counts: Mapping[str, int]) -> list[dict]:
    """Build a nested folder tree with subtree rule counts from leaf path counts."""
    root: dict[str, dict] = {}

    for folder_path, count in folder_counts.items():
        if not folder_path:
            continue

        current_level = root
        path_parts: list[str] = []
        for part in folder_path.split("/"):
            part = part.strip().lower()
            if not part:
                continue

            path_parts.append(part)
            path = "/".join(path_parts)
            node = current_level.setdefault(
                part,
                {
                    "name": part,
                    "path": path,
                    "rules_count": 0,
                    "children": {},
                },
            )
            node["rules_count"] += count
            current_level = node["children"]

    return _finalize_rule_folder_nodes(root)


def _finalize_rule_folder_nodes(nodes_by_name: Mapping[str, dict]) -> list[dict]:
    nodes = []
    for node in sorted(nodes_by_name.values(), key=_rule_folder_node_sort_key):
        children = _finalize_rule_folder_nodes(node["children"])
        nodes.append(
            {
                "path": node["path"],
                "rules_count": node["rules_count"],
                "children": children,
            }
        )
    return nodes


def _rule_folder_node_sort_key(node: dict) -> tuple[int, str]:
    name = node["name"]
    order_map = ROOT_FOLDER_ORDER if "/" not in node["path"] else CHILD_FOLDER_ORDER
    order = order_map.get(name, 100)
    return order, name.lower()


def extract_referenced_sigma_uuids(body: str | None) -> list[str]:
    """Return Sigma UUIDs referenced inside a high-order Sigma body, excluding the HOS' own id."""
    if not body:
        return []
    uuids = {match.group(0).lower() for match in UUID_PATTERN.finditer(body)}
    own_uuid = get_sigma_uuid_from_sigma_body(body)
    if own_uuid:
        uuids.discard(own_uuid)
    return sorted(uuids)


def _normalize_folder_segment(value: str | None) -> str | None:
    if not isinstance(value, str):
        return None
    trimmed = value.strip()
    if not trimmed:
        return None
    return trimmed.replace("/", "_").lower()


def compute_logsource_folder_path(
    product: str | None,
    service: str | None,
    category: str | None,
) -> str | None:
    """Compute the virtual folder path from Sigma logsource fields (non-GitLab repos)."""
    product_norm = _normalize_folder_segment(product)
    service_norm = _normalize_folder_segment(service)
    category_norm = _normalize_folder_segment(category)

    if product_norm and category_norm:
        return f"_product/{product_norm}/_category/{category_norm}"
    if product_norm and service_norm:
        return f"_product/{product_norm}/_service/{service_norm}"
    if product_norm:
        return f"_product/{product_norm}"
    if category_norm and service_norm:
        return f"_category/{category_norm}/_service/{service_norm}"
    if category_norm:
        return f"_category/{category_norm}"
    if service_norm:
        return f"_service/{service_norm}"
    return None


def gitlab_folder_path_from_file_path(file_path: str) -> str | None:
    """Return the directory portion of a GitLab subpath-relative rule file path."""
    normalized = file_path.strip("/")
    if not normalized or "/" not in normalized:
        return None
    return "/".join(normalized.split("/")[:-1])
