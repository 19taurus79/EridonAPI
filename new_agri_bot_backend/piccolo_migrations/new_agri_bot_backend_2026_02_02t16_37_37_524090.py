from piccolo.apps.migrations.auto.migration_manager import MigrationManager
from piccolo.columns.column_types import ForeignKey
from piccolo.columns.column_types import Integer
from piccolo.columns.column_types import Serial
from piccolo.columns.column_types import Text
from piccolo.columns.column_types import UUID
from piccolo.columns.defaults.uuid import UUID4
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


ID = "2026-02-02T16:37:37:524090"
VERSION = "1.26.1"
DESCRIPTION = ""


async def forwards():
    manager = MigrationManager(
        migration_id=ID, app_name="new_agri_bot_backend", description=DESCRIPTION
    )

    manager.add_column(
        table_class_name="OrderComments",
        tablename="order_comments",
        column_name="comment_text",
        db_column_name="comment_text",
        column_class_name="Text",
        column_class=Text,
        params={
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
        column_name="created_by",
        db_column_name="created_by",
        column_class_name="Integer",
        column_class=Integer,
        params={
            "default": 0,
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
        column_name="product_id",
        db_column_name="product_id",
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
