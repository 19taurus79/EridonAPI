from piccolo.apps.migrations.auto.migration_manager import MigrationManager
from piccolo.columns.column_types import Boolean
from piccolo.columns.indexes import IndexMethod


ID = "2025-07-31T16:15:06:744619"
VERSION = "1.26.1"
DESCRIPTION = ""


async def forwards():
    manager = MigrationManager(
        migration_id=ID, app_name="new_agri_bot_backend", description=DESCRIPTION
    )

    manager.add_column(
        table_class_name="DetailsForOrders",
        tablename="details_for_orders",
        column_name="qok",
        db_column_name="qok",
        column_class_name="Boolean",
        column_class=Boolean,
        params={
            "default": False,
            "null": False,
            "primary_key": False,
            "unique": False,
            "index": False,
            "index_method": IndexMethod.btree,
            "choices": None,
            "db_column_name": None,
            "secret": False,
        },
        schema=None,
    )

    return manager
