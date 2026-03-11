from piccolo.apps.migrations.auto.migration_manager import MigrationManager
from piccolo.columns.column_types import BigInt
from piccolo.columns.column_types import Boolean
from piccolo.columns.column_types import DoublePrecision
from piccolo.columns.column_types import ForeignKey
from piccolo.columns.column_types import Serial
from piccolo.columns.column_types import Varchar
from piccolo.columns.indexes import IndexMethod
from piccolo.table import Table


class AddressGuide(Table, tablename="address_guide", schema=None):
    id = Serial(
        null=False,
        primary_key=True,
        unique=False,
        index=False,
        index_method=IndexMethod.btree,
        choices=None,
        db_column_name="id",
        secret=False,
    )


ID = "2026-03-11T16:05:58:981150"
VERSION = "1.25.0"
DESCRIPTION = ""


async def forwards():
    manager = MigrationManager(
        migration_id=ID, app_name="new_agri_bot_backend", description=DESCRIPTION
    )

    manager.add_table(
        class_name="ValidFreeStock",
        tablename="valid_free_stock",
        schema=None,
        columns=None,
    )

    manager.add_column(
        table_class_name="ValidFreeStock",
        tablename="valid_free_stock",
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
        table_class_name="ValidFreeStock",
        tablename="valid_free_stock",
        column_name="division",
        db_column_name="division",
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
        table_class_name="ValidFreeStock",
        tablename="valid_free_stock",
        column_name="warehouse",
        db_column_name="warehouse",
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
        table_class_name="ValidFreeStock",
        tablename="valid_free_stock",
        column_name="free_qty",
        db_column_name="free_qty",
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
        table_class_name="ValidFreeStock",
        tablename="valid_free_stock",
        column_name="buh_qty",
        db_column_name="buh_qty",
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
        table_class_name="ValidFreeStock",
        tablename="valid_free_stock",
        column_name="skl_qty",
        db_column_name="skl_qty",
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
        table_class_name="Payment",
        tablename="payment",
        column_name="client",
        db_column_name="client",
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
        table_class_name="Users",
        tablename="users",
        column_name="is_guest",
        db_column_name="is_guest",
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

    manager.alter_column(
        table_class_name="AddressGuide",
        tablename="address_guide",
        column_name="level_1_id",
        db_column_name="level_1_id",
        params={"references": "self"},
        old_params={"references": AddressGuide},
        column_class=ForeignKey,
        old_column_class=ForeignKey,
        schema=None,
    )

    manager.alter_column(
        table_class_name="AddressGuide",
        tablename="address_guide",
        column_name="level_2_id",
        db_column_name="level_2_id",
        params={"references": "self"},
        old_params={"references": AddressGuide},
        column_class=ForeignKey,
        old_column_class=ForeignKey,
        schema=None,
    )

    manager.alter_column(
        table_class_name="AddressGuide",
        tablename="address_guide",
        column_name="level_3_id",
        db_column_name="level_3_id",
        params={"references": "self"},
        old_params={"references": AddressGuide},
        column_class=ForeignKey,
        old_column_class=ForeignKey,
        schema=None,
    )

    manager.alter_column(
        table_class_name="AddressGuide",
        tablename="address_guide",
        column_name="level_4_id",
        db_column_name="level_4_id",
        params={"references": "self"},
        old_params={"references": AddressGuide},
        column_class=ForeignKey,
        old_column_class=ForeignKey,
        schema=None,
    )

    manager.alter_column(
        table_class_name="AddressGuide",
        tablename="address_guide",
        column_name="level_5_id",
        db_column_name="level_5_id",
        params={"references": "self"},
        old_params={"references": AddressGuide},
        column_class=ForeignKey,
        old_column_class=ForeignKey,
        schema=None,
    )

    manager.alter_column(
        table_class_name="OrderChatMessage",
        tablename="order_chat_message",
        column_name="order_ref",
        db_column_name="order_ref",
        params={"index": False},
        old_params={"index": True},
        column_class=Varchar,
        old_column_class=Varchar,
        schema=None,
    )

    manager.alter_column(
        table_class_name="OrderChatMessage",
        tablename="order_chat_message",
        column_name="user_id",
        db_column_name="user_id",
        params={"index": False},
        old_params={"index": True},
        column_class=BigInt,
        old_column_class=BigInt,
        schema=None,
    )

    manager.alter_column(
        table_class_name="OrderChatMessage",
        tablename="order_chat_message",
        column_name="reply_to_message_id",
        db_column_name="reply_to_message_id",
        params={"target_column": None},
        old_params={"target_column": "id"},
        column_class=ForeignKey,
        old_column_class=ForeignKey,
        schema=None,
    )

    manager.alter_column(
        table_class_name="OrderComments",
        tablename="order_comments",
        column_name="order_ref",
        db_column_name="order_ref",
        params={"length": 255},
        old_params={"length": 50},
        column_class=Varchar,
        old_column_class=Varchar,
        schema=None,
    )

    return manager
