from piccolo.apps.migrations.auto.migration_manager import MigrationManager
from piccolo.columns.column_types import BigInt
from piccolo.columns.column_types import Integer


ID = "2025-08-27T17:52:01:484286"
VERSION = "1.26.1"
DESCRIPTION = ""


async def forwards():
    manager = MigrationManager(
        migration_id=ID, app_name="new_agri_bot_backend", description=DESCRIPTION
    )

    manager.alter_column(
        table_class_name="Events",
        tablename="events",
        column_name="event_creator",
        db_column_name="event_creator",
        params={},
        old_params={},
        column_class=BigInt,
        old_column_class=Integer,
        schema=None,
    )

    manager.alter_column(
        table_class_name="Events",
        tablename="events",
        column_name="event_who_changed_id",
        db_column_name="event_who_changed_id",
        params={},
        old_params={},
        column_class=BigInt,
        old_column_class=Integer,
        schema=None,
    )

    manager.alter_column(
        table_class_name="Tasks",
        tablename="tasks",
        column_name="task_creator",
        db_column_name="task_creator",
        params={},
        old_params={},
        column_class=BigInt,
        old_column_class=Integer,
        schema=None,
    )

    manager.alter_column(
        table_class_name="Tasks",
        tablename="tasks",
        column_name="task_who_changed_id",
        db_column_name="task_who_changed_id",
        params={},
        old_params={},
        column_class=BigInt,
        old_column_class=Integer,
        schema=None,
    )

    return manager
