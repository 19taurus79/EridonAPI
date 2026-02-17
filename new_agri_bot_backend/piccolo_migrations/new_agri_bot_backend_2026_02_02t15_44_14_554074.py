from piccolo.apps.migrations.auto.migration_manager import MigrationManager
from piccolo.columns.column_types import ForeignKey
from piccolo.columns.column_types import Serial
from piccolo.columns.column_types import Timestamp
from piccolo.columns.column_types import Varchar
from piccolo.columns.defaults.timestamp import TimestampNow
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


ID = "2026-02-02T15:44:14:554074"
VERSION = "1.26.1"
DESCRIPTION = ""


async def forwards():
    manager = MigrationManager(
        migration_id=ID, app_name="new_agri_bot_backend", description=DESCRIPTION
    )

    manager.add_table(
        class_name="OrderComments",
        tablename="order_comments",
        schema=None,
        columns=None,
    )

    manager.add_column(
        table_class_name="OrderComments",
        tablename="order_comments",
        column_name="comment_type",
        db_column_name="comment_type",
        column_class_name="Varchar",
        column_class=Varchar,
        params={
            "length": 10,
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
        table_class_name="OrderComments",
        tablename="order_comments",
        column_name="order_ref",
        db_column_name="order_ref",
        column_class_name="Varchar",
        column_class=Varchar,
        params={
            "length": 50,
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
        table_class_name="OrderComments",
        tablename="order_comments",
        column_name="product_name",
        db_column_name="product_name",
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
        table_class_name="OrderComments",
        tablename="order_comments",
        column_name="created_by_name",
        db_column_name="created_by_name",
        column_class_name="Varchar",
        column_class=Varchar,
        params={
            "length": 100,
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
        table_class_name="OrderComments",
        tablename="order_comments",
        column_name="created_at",
        db_column_name="created_at",
        column_class_name="Timestamp",
        column_class=Timestamp,
        params={
            "default": TimestampNow(),
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
        table_class_name="OrderComments",
        tablename="order_comments",
        column_name="updated_at",
        db_column_name="updated_at",
        column_class_name="Timestamp",
        column_class=Timestamp,
        params={
            "default": TimestampNow(),
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
        params={"references": AddressGuide},
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
        params={"references": AddressGuide},
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
        params={"references": AddressGuide},
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
        params={"references": AddressGuide},
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
        params={"references": AddressGuide},
        old_params={"references": AddressGuide},
        column_class=ForeignKey,
        old_column_class=ForeignKey,
        schema=None,
    )

    return manager
