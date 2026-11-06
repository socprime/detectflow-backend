"""update rule folder paths

Revision ID: d2e3f4a5b6c7
Revises: e8f9a0b1c2d4
Create Date: 2026-06-03

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "d2e3f4a5b6c7"
down_revision: str | Sequence[str] | None = "e8f9a0b1c2d4"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

NEW_FOLDER_PATH_EXPRESSION = """
CASE
    WHEN NULLIF(BTRIM(product), '') IS NOT NULL
     AND NULLIF(BTRIM(category), '') IS NOT NULL
        THEN '_product/' || LOWER(REPLACE(NULLIF(BTRIM(product), ''), '/', '_')) || '/_category/' || LOWER(REPLACE(NULLIF(BTRIM(category), ''), '/', '_'))
    WHEN NULLIF(BTRIM(product), '') IS NOT NULL
     AND NULLIF(BTRIM(service), '') IS NOT NULL
        THEN '_product/' || LOWER(REPLACE(NULLIF(BTRIM(product), ''), '/', '_')) || '/_service/' || LOWER(REPLACE(NULLIF(BTRIM(service), ''), '/', '_'))
    WHEN NULLIF(BTRIM(product), '') IS NOT NULL
        THEN '_product/' || LOWER(REPLACE(NULLIF(BTRIM(product), ''), '/', '_'))
    WHEN NULLIF(BTRIM(category), '') IS NOT NULL
     AND NULLIF(BTRIM(service), '') IS NOT NULL
        THEN '_category/' || LOWER(REPLACE(NULLIF(BTRIM(category), ''), '/', '_')) || '/_service/' || LOWER(REPLACE(NULLIF(BTRIM(service), ''), '/', '_'))
    WHEN NULLIF(BTRIM(category), '') IS NOT NULL
        THEN '_category/' || LOWER(REPLACE(NULLIF(BTRIM(category), ''), '/', '_'))
    WHEN NULLIF(BTRIM(service), '') IS NOT NULL
        THEN '_service/' || LOWER(REPLACE(NULLIF(BTRIM(service), ''), '/', '_'))
    ELSE NULL
END
"""

OLD_FOLDER_PATH_EXPRESSION = """
CASE
    WHEN NULLIF(BTRIM(product), '') IS NOT NULL
     AND NULLIF(BTRIM(category), '') IS NOT NULL
        THEN 'product/' || LOWER(REPLACE(NULLIF(BTRIM(product), ''), '/', '_')) || '/category/' || LOWER(REPLACE(NULLIF(BTRIM(category), ''), '/', '_'))
    WHEN NULLIF(BTRIM(product), '') IS NOT NULL
     AND NULLIF(BTRIM(service), '') IS NOT NULL
        THEN 'product/' || LOWER(REPLACE(NULLIF(BTRIM(product), ''), '/', '_')) || '/service/' || LOWER(REPLACE(NULLIF(BTRIM(service), ''), '/', '_'))
    WHEN NULLIF(BTRIM(product), '') IS NOT NULL
        THEN 'product/' || LOWER(REPLACE(NULLIF(BTRIM(product), ''), '/', '_')) || '/other'
    WHEN NULLIF(BTRIM(category), '') IS NOT NULL
     AND NULLIF(BTRIM(service), '') IS NOT NULL
        THEN 'category/' || LOWER(REPLACE(NULLIF(BTRIM(category), ''), '/', '_')) || '/service/' || LOWER(REPLACE(NULLIF(BTRIM(service), ''), '/', '_'))
    WHEN NULLIF(BTRIM(category), '') IS NOT NULL
        THEN 'category/' || LOWER(REPLACE(NULLIF(BTRIM(category), ''), '/', '_')) || '/other'
    WHEN NULLIF(BTRIM(service), '') IS NOT NULL
        THEN 'service/' || LOWER(REPLACE(NULLIF(BTRIM(service), ''), '/', '_'))
    ELSE 'generic'
END
"""

NEW_FOLDER_PATH_FUNCTION = """
CREATE OR REPLACE FUNCTION pg_temp._normalize_new_rule_folder_path(raw_path text)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    parts text[];
    part_count integer;
    normalized text[];
BEGIN
    IF raw_path IS NULL OR BTRIM(raw_path) = '' THEN
        RETURN NULL;
    END IF;

    parts := regexp_split_to_array(LOWER(BTRIM(raw_path)), '/+');
    part_count := array_length(parts, 1);

    IF part_count IS NULL OR part_count = 0 OR parts[1] = 'generic' THEN
        RETURN NULL;
    END IF;

    IF parts[1] IN ('_product', '_category', '_service') THEN
        RETURN array_to_string(parts, '/');
    END IF;

    IF parts[1] = 'product' THEN
        IF part_count = 1 THEN
            RETURN '_product';
        END IF;

        normalized := ARRAY['_product', parts[2]];

        IF part_count >= 3 THEN
            IF parts[3] = 'other' THEN
                RETURN array_to_string(normalized, '/');
            ELSIF parts[3] = 'category' THEN
                normalized := array_append(normalized, '_category');
            ELSIF parts[3] = 'service' THEN
                normalized := array_append(normalized, '_service');
            ELSE
                normalized := array_append(normalized, parts[3]);
            END IF;
        END IF;

        IF part_count >= 4 THEN
            normalized := normalized || parts[4:part_count];
        END IF;

        RETURN array_to_string(normalized, '/');
    END IF;

    IF parts[1] = 'category' THEN
        IF part_count = 1 THEN
            RETURN '_category';
        END IF;

        normalized := ARRAY['_category', parts[2]];

        IF part_count >= 3 THEN
            IF parts[3] = 'other' THEN
                RETURN array_to_string(normalized, '/');
            ELSIF parts[3] = 'service' THEN
                normalized := array_append(normalized, '_service');
            ELSE
                normalized := array_append(normalized, parts[3]);
            END IF;
        END IF;

        IF part_count >= 4 THEN
            normalized := normalized || parts[4:part_count];
        END IF;

        RETURN array_to_string(normalized, '/');
    END IF;

    IF parts[1] = 'service' THEN
        IF part_count = 1 THEN
            RETURN '_service';
        END IF;

        normalized := ARRAY['_service'] || parts[2:part_count];
        RETURN array_to_string(normalized, '/');
    END IF;

    RETURN array_to_string(parts, '/');
END;
$$;
"""

OLD_FOLDER_PATH_FUNCTION = """
CREATE OR REPLACE FUNCTION pg_temp._normalize_old_rule_folder_path(raw_path text)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    parts text[];
    part_count integer;
    normalized text[];
BEGIN
    IF raw_path IS NULL OR BTRIM(raw_path) = '' THEN
        RETURN NULL;
    END IF;

    parts := regexp_split_to_array(LOWER(BTRIM(raw_path)), '/+');
    part_count := array_length(parts, 1);

    IF part_count IS NULL OR part_count = 0 THEN
        RETURN NULL;
    END IF;

    IF parts[1] IN ('product', 'category', 'service', 'generic') THEN
        RETURN array_to_string(parts, '/');
    END IF;

    IF parts[1] = '_product' THEN
        IF part_count = 1 THEN
            RETURN 'product';
        END IF;

        normalized := ARRAY['product', parts[2]];

        IF part_count >= 3 THEN
            IF parts[3] = '_category' THEN
                normalized := array_append(normalized, 'category');
            ELSIF parts[3] = '_service' THEN
                normalized := array_append(normalized, 'service');
            ELSE
                normalized := array_append(normalized, parts[3]);
            END IF;
        END IF;

        IF part_count >= 4 THEN
            normalized := normalized || parts[4:part_count];
        END IF;

        RETURN array_to_string(normalized, '/');
    END IF;

    IF parts[1] = '_category' THEN
        IF part_count = 1 THEN
            RETURN 'category';
        END IF;

        normalized := ARRAY['category', parts[2]];

        IF part_count >= 3 THEN
            IF parts[3] = '_service' THEN
                normalized := array_append(normalized, 'service');
            ELSE
                normalized := array_append(normalized, parts[3]);
            END IF;
        END IF;

        IF part_count >= 4 THEN
            normalized := normalized || parts[4:part_count];
        END IF;

        RETURN array_to_string(normalized, '/');
    END IF;

    IF parts[1] = '_service' THEN
        IF part_count = 1 THEN
            RETURN 'service';
        END IF;

        normalized := ARRAY['service'] || parts[2:part_count];
        RETURN array_to_string(normalized, '/');
    END IF;

    RETURN array_to_string(parts, '/');
END;
$$;
"""

REWRITE_PIPELINE_SELECTIONS_SQL = """
UPDATE pipeline_repositories
SET folder_paths = COALESCE(
    (
        SELECT array_agg(normalized_path ORDER BY first_ord)
        FROM (
            SELECT normalized_path, MIN(ord) AS first_ord
            FROM unnest(folder_paths) WITH ORDINALITY AS folders(folder_path, ord)
            CROSS JOIN LATERAL (
                SELECT {normalize_function}(folder_path) AS normalized_path
            ) normalized
            WHERE normalized_path IS NOT NULL
            GROUP BY normalized_path
        ) deduped
    ),
    '{{}}'::text[]
)
WHERE folder_paths IS NOT NULL
"""

REWRITE_LOG_SOURCE_SELECTIONS_SQL = """
UPDATE log_sources
SET test_repository_ids = (
    SELECT COALESCE(
        jsonb_agg(
            jsonb_set(selection, '{{folders}}', normalized.folders, true)
            ORDER BY ord
        ),
        '[]'::jsonb
    )
    FROM jsonb_array_elements(
        CASE
            WHEN jsonb_typeof(test_repository_ids) = 'array' THEN test_repository_ids
            ELSE '[]'::jsonb
        END
    ) WITH ORDINALITY AS selections(selection, ord)
    CROSS JOIN LATERAL (
        SELECT COALESCE(jsonb_agg(to_jsonb(normalized_path) ORDER BY first_ord), '[]'::jsonb) AS folders
        FROM (
            SELECT normalized_path, MIN(folder_ord) AS first_ord
            FROM jsonb_array_elements_text(
                CASE
                    WHEN jsonb_typeof(selection->'folders') = 'array' THEN selection->'folders'
                    ELSE '[]'::jsonb
                END
            )
                WITH ORDINALITY AS folders(folder_path, folder_ord)
            CROSS JOIN LATERAL (
                SELECT {normalize_function}(folder_path) AS normalized_path
            ) normalized_paths
            WHERE normalized_path IS NOT NULL
            GROUP BY normalized_path
        ) deduped
    ) normalized
)
WHERE test_repository_ids IS NOT NULL
  AND jsonb_typeof(test_repository_ids) = 'array'
"""


def _replace_rule_folder_path(expression: str, nullable: bool) -> None:
    op.drop_index("ix_rules_repository_folder_path", table_name="rules")
    op.drop_column("rules", "folder_path")
    op.execute(
        f"""
        ALTER TABLE rules
        ADD COLUMN folder_path TEXT GENERATED ALWAYS AS ({expression}) STORED
        """
    )
    if not nullable:
        op.execute("ALTER TABLE rules ALTER COLUMN folder_path SET NOT NULL")
    op.create_index("ix_rules_repository_folder_path", "rules", ["repository_id", "folder_path"])


def _rewrite_stored_folder_paths(normalize_function: str) -> None:
    op.execute(REWRITE_PIPELINE_SELECTIONS_SQL.format(normalize_function=normalize_function))
    op.execute(REWRITE_LOG_SOURCE_SELECTIONS_SQL.format(normalize_function=normalize_function))


def upgrade() -> None:
    """Use prefixed folder type segments and keep unmatched rules at repository root."""
    op.execute(NEW_FOLDER_PATH_FUNCTION)
    _rewrite_stored_folder_paths("pg_temp._normalize_new_rule_folder_path")
    _replace_rule_folder_path(NEW_FOLDER_PATH_EXPRESSION, nullable=True)


def downgrade() -> None:
    """Restore legacy folder paths with other/generic buckets."""
    op.execute(OLD_FOLDER_PATH_FUNCTION)
    _rewrite_stored_folder_paths("pg_temp._normalize_old_rule_folder_path")
    _replace_rule_folder_path(OLD_FOLDER_PATH_EXPRESSION, nullable=False)
