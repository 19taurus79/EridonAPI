from piccolo.apps.migrations.auto.migration_manager import MigrationManager
from piccolo.columns.column_types import DoublePrecision
from piccolo.columns.column_types import Integer
from piccolo.columns.indexes import IndexMethod


ID = "2026-06-15T12:14:59:285377"
VERSION = "1.26.1"
DESCRIPTION = ""


async def forwards():
    manager = MigrationManager(
        migration_id=ID, app_name="new_agri_bot_backend", description=DESCRIPTION
    )

    manager.add_column(
        table_class_name="ClientAddress",
        tablename="client_address",
        column_name="default_car_height",
        db_column_name="default_car_height",
        column_class_name="DoublePrecision",
        column_class=DoublePrecision,
        params={
            "default": 0.0,
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
        table_class_name="ClientAddress",
        tablename="client_address",
        column_name="default_car_length",
        db_column_name="default_car_length",
        column_class_name="DoublePrecision",
        column_class=DoublePrecision,
        params={
            "default": 0.0,
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
        table_class_name="ClientAddress",
        tablename="client_address",
        column_name="default_car_max_weight",
        db_column_name="default_car_max_weight",
        column_class_name="Integer",
        column_class=Integer,
        params={
            "default": 0,
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
        table_class_name="ClientAddress",
        tablename="client_address",
        column_name="default_car_own_weight",
        db_column_name="default_car_own_weight",
        column_class_name="Integer",
        column_class=Integer,
        params={
            "default": 0,
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
        table_class_name="ClientAddress",
        tablename="client_address",
        column_name="default_car_width",
        db_column_name="default_car_width",
        column_class_name="DoublePrecision",
        column_class=DoublePrecision,
        params={
            "default": 0.0,
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
