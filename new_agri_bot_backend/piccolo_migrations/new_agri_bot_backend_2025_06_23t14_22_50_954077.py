from piccolo.apps.migrations.auto.migration_manager import MigrationManager
from piccolo.columns.column_types import BigInt
from piccolo.columns.column_types import Varchar
from piccolo.columns.indexes import IndexMethod


ID = "2025-06-23T14:22:50:954077"
VERSION = "1.26.1"
DESCRIPTION = ""


async def forwards():
    manager = MigrationManager(
        migration_id=ID, app_name="new_agri_bot_backend", description=DESCRIPTION
    )

    manager.add_table(
        class_name="ProductOnWarehouse",
        tablename="product_on_warehouse",
        schema=None,
        columns=None,
    )

    manager.add_column(
        table_class_name="ProductOnWarehouse",
        tablename="product_on_warehouse",
        column_name="id",
        db_column_name="id",
        column_class_name="BigInt",
        column_class=BigInt,
        params={
            "default": 0,
            "null": False,
            "primary_key": True,
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
        table_class_name="ProductOnWarehouse",
        tablename="product_on_warehouse",
        column_name="product",
        db_column_name="product",
        column_class_name="Varchar",
        column_class=Varchar,
        params={
            "length": 255,
            "default": "",
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
        table_class_name="ProductOnWarehouse",
        tablename="product_on_warehouse",
        column_name="line_of_business",
        db_column_name="line_of_business",
        column_class_name="Varchar",
        column_class=Varchar,
        params={
            "length": 255,
            "default": "",
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
