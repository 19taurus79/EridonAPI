from piccolo.apps.migrations.auto.migration_manager import MigrationManager
from piccolo.columns.column_types import DoublePrecision
from piccolo.columns.column_types import Numeric
from piccolo.columns.column_types import UUID
from piccolo.columns.column_types import Varchar
from piccolo.columns.defaults.uuid import UUID4
from piccolo.columns.indexes import IndexMethod
import decimal


ID = "2025-07-31T15:40:20:872425"
VERSION = "1.26.1"
DESCRIPTION = ""


async def forwards():
    manager = MigrationManager(
        migration_id=ID, app_name="new_agri_bot_backend", description=DESCRIPTION
    )

    manager.add_table(
        class_name="DetailsForOrders",
        tablename="details_for_orders",
        schema=None,
        columns=None,
    )

    manager.add_column(
        table_class_name="DetailsForOrders",
        tablename="details_for_orders",
        column_name="nomenclature",
        db_column_name="nomenclature",
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
        table_class_name="DetailsForOrders",
        tablename="details_for_orders",
        column_name="party_sign",
        db_column_name="party_sign",
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
        table_class_name="DetailsForOrders",
        tablename="details_for_orders",
        column_name="buying_season",
        db_column_name="buying_season",
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
        table_class_name="DetailsForOrders",
        tablename="details_for_orders",
        column_name="different",
        db_column_name="different",
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
        table_class_name="DetailsForOrders",
        tablename="details_for_orders",
        column_name="client",
        db_column_name="client",
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
        table_class_name="DetailsForOrders",
        tablename="details_for_orders",
        column_name="contract_supplement",
        db_column_name="contract_supplement",
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
        table_class_name="DetailsForOrders",
        tablename="details_for_orders",
        column_name="manager",
        db_column_name="manager",
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
        table_class_name="DetailsForOrders",
        tablename="details_for_orders",
        column_name="product",
        db_column_name="product",
        column_class_name="UUID",
        column_class=UUID,
        params={
            "default": UUID4(),
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
        table_class_name="DetailsForOrders",
        tablename="details_for_orders",
        column_name="orders_q",
        db_column_name="orders_q",
        column_class_name="Numeric",
        column_class=Numeric,
        params={
            "default": decimal.Decimal("0"),
            "digits": None,
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
        table_class_name="DetailsForOrders",
        tablename="details_for_orders",
        column_name="moved_q",
        db_column_name="moved_q",
        column_class_name="Numeric",
        column_class=Numeric,
        params={
            "default": decimal.Decimal("0"),
            "digits": None,
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
        table_class_name="DetailsForOrders",
        tablename="details_for_orders",
        column_name="party",
        db_column_name="party",
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
        table_class_name="DetailsForOrders",
        tablename="details_for_orders",
        column_name="buh",
        db_column_name="buh",
        column_class_name="Numeric",
        column_class=Numeric,
        params={
            "default": decimal.Decimal("0"),
            "digits": None,
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
        table_class_name="DetailsForOrders",
        tablename="details_for_orders",
        column_name="skl",
        db_column_name="skl",
        column_class_name="Numeric",
        column_class=Numeric,
        params={
            "default": decimal.Decimal("0"),
            "digits": None,
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
        table_class_name="DetailsForOrders",
        tablename="details_for_orders",
        column_name="id",
        db_column_name="id",
        column_class_name="UUID",
        column_class=UUID,
        params={
            "default": UUID4(),
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
