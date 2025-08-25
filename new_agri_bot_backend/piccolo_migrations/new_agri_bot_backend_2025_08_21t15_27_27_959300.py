from piccolo.apps.migrations.auto.migration_manager import MigrationManager
from piccolo.columns.column_types import Integer
from piccolo.columns.column_types import Numeric
import decimal


ID = "2025-08-21T15:27:27:959300"
VERSION = "1.26.1"
DESCRIPTION = ""


async def forwards():
    manager = MigrationManager(
        migration_id=ID, app_name="new_agri_bot_backend", description=DESCRIPTION
    )

    manager.alter_column(
        table_class_name="Events",
        tablename="events",
        column_name="event_status",
        db_column_name="event_status",
        params={"default": 0},
        old_params={"default": decimal.Decimal("0")},
        column_class=Integer,
        old_column_class=Numeric,
        schema=None,
    )

    manager.alter_column(
        table_class_name="Tasks",
        tablename="tasks",
        column_name="task_status",
        db_column_name="task_status",
        params={"default": 0},
        old_params={"default": decimal.Decimal("0")},
        column_class=Integer,
        old_column_class=Numeric,
        schema=None,
    )

    return manager
