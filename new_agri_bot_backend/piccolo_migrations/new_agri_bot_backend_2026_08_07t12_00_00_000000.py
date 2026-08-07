from piccolo.apps.migrations.auto.migration_manager import MigrationManager
from piccolo.columns.column_types import Varchar


ID = "2026-08-07T12:00:00:000000"
VERSION = "1.107.0"
DESCRIPTION = "Add ttn column to Deliveries table"


async def forwards():
    manager = MigrationManager(
        migration_id=ID, app_name="new_agri_bot_backend", description=DESCRIPTION
    )

    manager.add_column(
        table_class_name="Deliveries",
        tablename="deliveries",
        column_name="ttn",
        db_column_name="ttn",
        column_class_name="Varchar",
        column_class=Varchar,
        params={
            "length": 255,
            "default": "",
            "null": True,
            "primary_key": False,
            "unique": False,
            "index": False,
            "index_method": "btree",
            "choices": None,
            "db_column_name": None,
            "secret": False,
        },
        schema=None,
    )

    return manager
