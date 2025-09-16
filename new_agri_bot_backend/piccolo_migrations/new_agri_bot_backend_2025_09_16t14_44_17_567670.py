from piccolo.apps.migrations.auto.migration_manager import MigrationManager
from piccolo.columns.column_types import Boolean
from piccolo.columns.column_types import Varchar


ID = "2025-09-16T14:44:17:567670"
VERSION = "1.26.1"
DESCRIPTION = ""


async def forwards():
    manager = MigrationManager(
        migration_id=ID, app_name="new_agri_bot_backend", description=DESCRIPTION
    )

    manager.alter_column(
        table_class_name="DetailsForOrders",
        tablename="details_for_orders",
        column_name="qok",
        db_column_name="qok",
        params={"length": 255, "default": ""},
        old_params={"length": None, "default": False},
        column_class=Varchar,
        old_column_class=Boolean,
        schema=None,
    )

    return manager
