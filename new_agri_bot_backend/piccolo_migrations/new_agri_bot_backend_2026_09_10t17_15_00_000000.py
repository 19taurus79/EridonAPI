from piccolo.apps.migrations.auto.migration_manager import MigrationManager
from piccolo.columns.column_types import Boolean, Timestamp, Varchar
from piccolo.columns.indexes import IndexMethod


ID = "2026-09-10T17:15:00:000000"
VERSION = "1.26.1"
DESCRIPTION = "Add is_received, received_at, np_status, and np_status_code columns to Deliveries table"


async def forwards():
    manager = MigrationManager(
        migration_id=ID, app_name="new_agri_bot_backend", description=DESCRIPTION
    )

    manager.add_column(
        table_class_name="Deliveries",
        tablename="deliveries",
        column_name="is_received",
        db_column_name="is_received",
        column_class_name="Boolean",
        column_class=Boolean,
        params={
            "default": False,
            "null": False,
            "primary_key": False,
            "unique": False,
            "index": True,
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
        column_name="received_at",
        db_column_name="received_at",
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
        column_name="np_status",
        db_column_name="np_status",
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

    manager.add_column(
        table_class_name="Deliveries",
        tablename="deliveries",
        column_name="np_status_code",
        db_column_name="np_status_code",
        column_class_name="Varchar",
        column_class=Varchar,
        params={
            "length": 10,
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

    async def mark_old_deliveries_as_received():
        from piccolo.engine.finder import engine_finder
        engine = engine_finder()
        if engine:
            await engine.run_ddl(
                """
                UPDATE deliveries 
                SET is_received = TRUE 
                WHERE status = 'Виконано' 
                   OR delivery_date < CURRENT_DATE - INTERVAL '3 days'
                   OR (delivery_date IS NULL AND created_at < CURRENT_TIMESTAMP - INTERVAL '3 days');
                """
            )

    manager.add_raw(mark_old_deliveries_as_received)

    return manager
