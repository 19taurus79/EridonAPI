from piccolo.apps.migrations.auto.migration_manager import MigrationManager
from piccolo.columns.column_types import BigInt, Boolean, Timestamptz, UUID, Varchar
from piccolo.columns.defaults.timestamptz import TimestamptzNow
from piccolo.columns.defaults.uuid import UUID4
from piccolo.table import Table

ID = "2026-10-08T17:00:00:000000"
VERSION = "1.107.0"
DESCRIPTION = "Create UserOAuthLink table for ChatGPT MCP identity binding"


class UserOAuthLink(Table, tablename="user_oauth_links", schema=None):
    id = UUID(
        default=UUID4(),
        null=False,
        primary_key=True,
        unique=False,
        index=False,
    )
    telegram_id = BigInt(
        default=0,
        null=False,
        primary_key=False,
        unique=False,
        index=True,
    )
    oauth_sub = Varchar(
        length=255,
        default="",
        null=False,
        primary_key=False,
        unique=False,
        index=True,
    )
    issuer = Varchar(
        length=255,
        default="",
        null=False,
        primary_key=False,
        unique=False,
        index=False,
    )
    created_at = Timestamptz(
        default=TimestamptzNow(),
        null=False,
        primary_key=False,
        unique=False,
        index=False,
    )
    revoked_at = Timestamptz(
        default=None,
        null=True,
        primary_key=False,
        unique=False,
        index=False,
    )
    is_active = Boolean(
        default=True,
        null=False,
        primary_key=False,
        unique=False,
        index=False,
    )


async def forwards():
    manager = MigrationManager(
        migration_id=ID,
        app_name="new_agri_bot_backend",
        description=DESCRIPTION,
    )

    manager.add_table(
        class_name="UserOAuthLink",
        tablename="user_oauth_links",
        schema=None,
        columns=[
            UserOAuthLink.id,
            UserOAuthLink.telegram_id,
            UserOAuthLink.oauth_sub,
            UserOAuthLink.issuer,
            UserOAuthLink.created_at,
            UserOAuthLink.revoked_at,
            UserOAuthLink.is_active,
        ],
    )

    return manager
