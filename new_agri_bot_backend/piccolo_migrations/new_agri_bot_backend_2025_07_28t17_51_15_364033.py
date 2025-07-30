from piccolo.apps.migrations.auto.migration_manager import MigrationManager
from piccolo.columns.column_types import BigInt
from piccolo.columns.column_types import Boolean
from piccolo.columns.column_types import DoublePrecision
from piccolo.columns.column_types import Varchar
from piccolo.columns.indexes import IndexMethod


ID = "2025-07-28T17:51:15:364033"
VERSION = "1.26.1"
DESCRIPTION = ""


async def forwards():
    manager = MigrationManager(
        migration_id=ID, app_name="new_agri_bot_backend", description=DESCRIPTION
    )

    manager.add_table(
        class_name="ProductsForOrders",
        tablename="products_for_orders",
        schema=None,
        columns=None,
    )

    manager.add_column(
        table_class_name="ProductsForOrders",
        tablename="products_for_orders",
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
        table_class_name="ProductsForOrders",
        tablename="products_for_orders",
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
        table_class_name="ProductsForOrders",
        tablename="products_for_orders",
        column_name="order_q",
        db_column_name="order_q",
        column_class_name="DoublePrecision",
        column_class=DoublePrecision,
        params={
            "default": 0.0,
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
        table_class_name="ProductsForOrders",
        tablename="products_for_orders",
        column_name="remain_q",
        db_column_name="remain_q",
        column_class_name="DoublePrecision",
        column_class=DoublePrecision,
        params={
            "default": 0.0,
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
        table_class_name="ProductsForOrders",
        tablename="products_for_orders",
        column_name="enough",
        db_column_name="enough",
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
