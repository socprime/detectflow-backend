#!/usr/bin/env python3
"""
CLI script to reset user password.
Use this when all admins are locked out and cannot access the UI.

Usage:
    python scripts/reset_password.py --email admin@soc.local
    python scripts/reset_password.py --email admin@soc.local --password mypassword
"""

import argparse
import asyncio
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from apps.core.auth import generate_temporary_password, hash_password
from apps.core.settings import settings
from apps.modules.postgre.user import UserDAO


async def reset_password(email: str, new_password: str | None = None) -> None:
    """Reset user password by email."""
    engine = create_async_engine(settings.database_url)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with async_session() as session:
        user_dao = UserDAO(session)

        # Find user by email
        user = await user_dao.get_by_email(email)
        if not user:
            print(f"Error: User with email '{email}' not found")
            await engine.dispose()
            sys.exit(1)

        # Generate or use provided password
        if new_password:
            password = new_password
        else:
            password = generate_temporary_password()

        # Update password
        hashed = hash_password(password)
        await user_dao.update_password(user.id, hashed, must_change=True)
        await session.commit()

        print(f"Password reset successfully for: {email}")
        print(f"New temporary password: {password}")
        print("User must change password on next login.")

    await engine.dispose()


def main():
    parser = argparse.ArgumentParser(description="Reset user password for admin-panel-backend")
    parser.add_argument(
        "--email",
        "-e",
        required=True,
        help="Email of the user to reset password for",
    )
    parser.add_argument(
        "--password",
        "-p",
        required=False,
        help="New password (optional, will generate if not provided)",
    )

    args = parser.parse_args()

    asyncio.run(reset_password(args.email, args.password))


if __name__ == "__main__":
    main()
