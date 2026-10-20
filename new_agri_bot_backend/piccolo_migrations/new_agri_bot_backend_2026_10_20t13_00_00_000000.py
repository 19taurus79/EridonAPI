from piccolo.apps.migrations.auto.migration_manager import MigrationManager
from piccolo.columns.column_types import Boolean, Timestamp, Varchar
from piccolo.columns.indexes import IndexMethod


ID = "2026-10-20T13:00:00:000000"
VERSION = "1.26.1"
DESCRIPTION = "Add is_accountant_sent, accountant_sent_at, and accountant_name columns to Deliveries table"


async def forwards():
    manager = MigrationManager(
        migration_id=ID, app_name="new_agri_bot_backend", description=DESCRIPTION
    )

    manager.add_column(
        table_class_name="Deliveries",
        tablename="deliveries",
        column_name="is_accountant_sent",
        db_column_name="is_accountant_sent",
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

    manager.add_column(
        table_class_name="Deliveries",
        tablename="deliveries",
        column_name="accountant_sent_at",
        db_column_name="accountant_sent_at",
        column_class_name="Timestamp",
        column_class=Timestamp,
        params={
            "default": None,
            "null": True,
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

    manager.add_column(
        table_class_name="Deliveries",
        tablename="deliveries",
        column_name="accountant_name",
        db_column_name="accountant_name",
        column_class_name="Varchar",
        column_class=Varchar,
        params={
            "length": 255,
            "default": "",
            "null": True,
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
