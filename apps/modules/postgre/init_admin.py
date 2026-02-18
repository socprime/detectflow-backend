"""Default admin user initialization.

This module ensures a default admin user exists on first startup,
similar to how Alembic ensures database schema is up to date.

Usage from entrypoint.sh:
    python -m apps.modules.postgre.init_admin
"""

import asyncio
import sys

from apps.core.auth import hash_password
from apps.core.database import AsyncSessionLocal
from apps.core.enums import UserRole
from apps.modules.postgre.user import UserDAO

# Default admin credentials
DEFAULT_ADMIN_EMAIL = "admin@soc.local"
DEFAULT_ADMIN_PASSWORD = "admin"
DEFAULT_ADMIN_NAME = "Admin User"


async def create_default_admin() -> bool:
    """Create default admin user if no users exist.

    Returns:
        True if admin was created, False if users already exist.
    """
    async with AsyncSessionLocal() as session:
        user_dao = UserDAO(session)
        count = await user_dao.count()

        if count > 0:
            print(f"Found {count} existing users, skipping default admin creation")
            return False

        print("Creating default admin user...")
        await user_dao.create(
            full_name=DEFAULT_ADMIN_NAME,
            email=DEFAULT_ADMIN_EMAIL,
            password=hash_password(DEFAULT_ADMIN_PASSWORD),
            is_active=True,
            role=UserRole.ADMIN,
            must_change_password=True,
        )
        await session.commit()

        print(f"Default admin created: {DEFAULT_ADMIN_EMAIL} / {DEFAULT_ADMIN_PASSWORD}")
        print("WARNING: Please change the password after first login!")
        return True


def main() -> int:
    """CLI entry point for admin user initialization.

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    print("Checking for default admin user...")

    try:
        asyncio.run(create_default_admin())
        return 0
    except Exception as e:
        print(f"ERROR: Failed to initialize admin user: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
