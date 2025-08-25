from piccolo.apps.migrations.auto.migration_manager import MigrationManager
from piccolo.columns.column_types import Integer
from piccolo.columns.column_types import Varchar


ID = "2025-08-21T15:38:01:547372"
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
        params={"default": 0},
        old_params={"default": ""},
        column_class=Integer,
        old_column_class=Varchar,
        schema=None,
    )

    manager.alter_column(
        table_class_name="Tasks",
        tablename="tasks",
        column_name="task_creator",
        db_column_name="task_creator",
        params={"default": 0},
        old_params={"default": ""},
        column_class=Integer,
        old_column_class=Varchar,
        schema=None,
    )

    return manager
