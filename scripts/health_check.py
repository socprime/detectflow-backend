#!/usr/bin/env python3
"""
CLI script to run health checks for selected platforms (or all if none specified).

Registered platforms: PostgreSQL, Cloud Repositories, Kafka.

Platform names accept aliases (case-insensitive for lowercase form):
  - PostgreSQL: "PostgreSQL" or "postgresql"
  - Cloud Repositories: "Cloud Repositories", "cloud_repositories", or "cloud repositories"
  - Kafka: "Kafka" or "kafka"

Usage:
    # Run checks for all registered platforms
    python scripts/health_check.py

    # Run checks only for given platforms
    python scripts/health_check.py --platforms PostgreSQL
    python scripts/health_check.py --platforms postgresql cloud_repositories kafka

"""

import argparse
import asyncio
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from apps.core.database import engine
from apps.services.health_check.main import health_check_service

# Aliases for platform names (lowercase key → canonical name used by the service)
_PLATFORM_ALIASES: dict[str, str] = {
    "postgresql": "PostgreSQL",
    "cloud_repositories": "Cloud Repositories",
    "kafka": "Kafka",
}


def _normalize_platform(name: str) -> str:
    """Convert user input to canonical platform name.

    Accepts: PostgreSQL, postgresql; Cloud Repositories, cloud_repositories;
    Kafka, kafka.
    """
    s = name.strip()
    key = s.lower()
    if key in _PLATFORM_ALIASES:
        return _PLATFORM_ALIASES[key]
    if s in _PLATFORM_ALIASES.values():
        return s
    raise ValueError(f"Unknown platform: {name}")


def _format_check(check: dict) -> str:
    """Format a single check result for console output."""
    status = check.get("status", "?")
    title = check.get("title", "")
    descriptions = check.get("descriptions") or []
    lines = [f"  [{status}] {title}"]
    for d in descriptions:
        lines.append(f"      {d}")
    return "\n".join(lines)


async def run_checks(platforms: list[str] | None) -> None:
    """Run health checks for given platforms (or all if platforms is None/empty) and print results.

    platforms must be canonical names (e.g. 'PostgreSQL', 'Cloud Repositories', 'Kafka');
    unknown names are rejected earlier in main() via _normalize_platform().
    """
    rows = await health_check_service.check_platforms(platforms)

    if not rows:
        print("No platforms checked.")
        return

    for row in rows:
        print(f"\n{row.name}")
        print("-" * 40)
        checks = row.checks if isinstance(row.checks, list) else []
        for c in checks:
            if not isinstance(c, dict):
                continue
            print(_format_check(c))
    print()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run health checks for specified platforms (or all if none given).")
    parser.add_argument(
        "--platforms",
        nargs="*",
        metavar="NAME",
        help="Platform names: PostgreSQL|postgresql, Cloud Repositories|cloud_repositories, Kafka|kafka. Omit to check all.",
    )
    args = parser.parse_args()

    # None or empty list → check all platforms; otherwise normalize and deduplicate
    if args.platforms:
        normalized = [_normalize_platform(p) for p in args.platforms]
        platforms = list(dict.fromkeys(normalized))
    else:
        platforms = None

    async def _run() -> None:
        try:
            await run_checks(platforms)
        except ValueError as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
        finally:
            await engine.dispose()

    asyncio.run(_run())


if __name__ == "__main__":
    main()
