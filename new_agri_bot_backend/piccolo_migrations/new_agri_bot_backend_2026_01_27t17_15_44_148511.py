from piccolo.apps.migrations.auto.migration_manager import MigrationManager
from piccolo.columns.column_types import Boolean
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


ID = "2026-01-27T17:15:44:148511"
VERSION = "1.26.1"
DESCRIPTION = ""


async def forwards():
    manager = MigrationManager(
        migration_id=ID, app_name="new_agri_bot_backend", description=DESCRIPTION
    )

    manager.add_column(
        table_class_name="Deliveries",
        tablename="deliveries",
        column_name="calendar_id",
        db_column_name="calendar_id",
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
        table_class_name="MovedData",
        tablename="moved_data",
        column_name="is_active",
        db_column_name="is_active",
        column_class_name="Boolean",
        column_class=Boolean,
        params={
            "default": True,
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
