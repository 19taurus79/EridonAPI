from piccolo.apps.migrations.auto.migration_manager import MigrationManager
from piccolo.columns.column_types import Timestamp
from piccolo.columns.column_types import Timestamptz
from piccolo.columns.defaults.timestamp import TimestampNow
from piccolo.columns.defaults.timestamptz import TimestamptzNow


ID = "2025-09-14T21:22:13:527369"
VERSION = "1.26.1"
DESCRIPTION = ""


async def forwards():
    manager = MigrationManager(
        migration_id=ID, app_name="new_agri_bot_backend", description=DESCRIPTION
    )

    manager.alter_column(
        table_class_name="Events",
        tablename="events",
        column_name="updated_at",
        db_column_name="updated_at",
        params={"default": TimestampNow(), "null": False},
        old_params={"default": TimestamptzNow(), "null": True},
        column_class=Timestamp,
        old_column_class=Timestamptz,
        schema=None,
    )

    manager.alter_column(
        table_class_name="Tasks",
        tablename="tasks",
        column_name="updated_at",
        db_column_name="updated_at",
        params={"default": TimestampNow(), "null": False},
        old_params={"default": TimestamptzNow(), "null": True},
        column_class=Timestamp,
        old_column_class=Timestamptz,
        schema=None,
    )

    return manager
