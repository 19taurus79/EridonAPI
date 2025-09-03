from piccolo.apps.migrations.auto.migration_manager import MigrationManager
from piccolo.columns.column_types import Timestamp
from piccolo.columns.column_types import Timestamptz
from piccolo.columns.defaults.timestamp import TimestampNow
from piccolo.columns.defaults.timestamptz import TimestamptzNow


ID = "2025-09-03T23:04:03:993849"
VERSION = "1.26.1"
DESCRIPTION = ""


async def forwards():
    manager = MigrationManager(
        migration_id=ID, app_name="new_agri_bot_backend", description=DESCRIPTION
    )

    manager.alter_column(
        table_class_name="Events",
        tablename="events",
        column_name="created_at",
        db_column_name="created_at",
        params={"default": TimestamptzNow()},
        old_params={"default": TimestampNow()},
        column_class=Timestamptz,
        old_column_class=Timestamp,
        schema=None,
    )

    manager.alter_column(
        table_class_name="Events",
        tablename="events",
        column_name="updated_at",
        db_column_name="updated_at",
        params={"default": TimestamptzNow()},
        old_params={"default": TimestampNow()},
        column_class=Timestamptz,
        old_column_class=Timestamp,
        schema=None,
    )

    manager.alter_column(
        table_class_name="Tasks",
        tablename="tasks",
        column_name="created_at",
        db_column_name="created_at",
        params={"default": TimestamptzNow()},
        old_params={"default": TimestampNow()},
        column_class=Timestamptz,
        old_column_class=Timestamp,
        schema=None,
    )

    manager.alter_column(
        table_class_name="Tasks",
        tablename="tasks",
        column_name="updated_at",
        db_column_name="updated_at",
        params={"default": TimestamptzNow()},
        old_params={"default": TimestampNow()},
        column_class=Timestamptz,
        old_column_class=Timestamp,
        schema=None,
    )

    return manager
