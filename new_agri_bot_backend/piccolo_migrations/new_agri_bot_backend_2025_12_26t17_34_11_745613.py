from piccolo.apps.migrations.auto.migration_manager import MigrationManager
from piccolo.columns.base import OnDelete
from piccolo.columns.base import OnUpdate
from piccolo.columns.column_types import BigInt
from piccolo.columns.column_types import Boolean
from piccolo.columns.column_types import Date
from piccolo.columns.column_types import ForeignKey
from piccolo.columns.column_types import Real
from piccolo.columns.column_types import Serial
from piccolo.columns.column_types import Text
from piccolo.columns.column_types import Timestamp
from piccolo.columns.column_types import Varchar
from piccolo.columns.defaults.date import DateNow
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


class Deliveries(Table, tablename="deliveries", schema=None):
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


ID = "2025-12-26T17:34:11:745613"
VERSION = "1.26.1"
DESCRIPTION = ""


async def forwards():
    manager = MigrationManager(
        migration_id=ID, app_name="new_agri_bot_backend", description=DESCRIPTION
    )

    manager.add_table(
        class_name="DeliveryItems",
        tablename="delivery_items",
        schema=None,
        columns=None,
    )

    manager.add_table(
        class_name="Deliveries", tablename="deliveries", schema=None, columns=None
    )

    manager.add_column(
        table_class_name="DeliveryItems",
        tablename="delivery_items",
        column_name="delivery",
        db_column_name="delivery",
        column_class_name="ForeignKey",
        column_class=ForeignKey,
        params={
            "references": Deliveries,
            "on_delete": OnDelete.cascade,
            "on_update": OnUpdate.cascade,
            "target_column": None,
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
        table_class_name="DeliveryItems",
        tablename="delivery_items",
        column_name="order_ref",
        db_column_name="order_ref",
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
        table_class_name="DeliveryItems",
        tablename="delivery_items",
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
        table_class_name="DeliveryItems",
        tablename="delivery_items",
        column_name="quantity",
        db_column_name="quantity",
        column_class_name="Real",
        column_class=Real,
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
        table_class_name="DeliveryItems",
        tablename="delivery_items",
        column_name="party",
        db_column_name="party",
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
        table_class_name="DeliveryItems",
        tablename="delivery_items",
        column_name="party_quantity",
        db_column_name="party_quantity",
        column_class_name="Real",
        column_class=Real,
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
        table_class_name="Deliveries",
        tablename="deliveries",
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
        table_class_name="Deliveries",
        tablename="deliveries",
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
        table_class_name="Deliveries",
        tablename="deliveries",
        column_name="address",
        db_column_name="address",
        column_class_name="Text",
        column_class=Text,
        params={
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
        column_name="contact",
        db_column_name="contact",
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
        column_name="phone",
        db_column_name="phone",
        column_class_name="Varchar",
        column_class=Varchar,
        params={
            "length": 50,
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
        column_name="delivery_date",
        db_column_name="delivery_date",
        column_class_name="Date",
        column_class=Date,
        params={
            "default": DateNow(),
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
        column_name="comment",
        db_column_name="comment",
        column_class_name="Text",
        column_class=Text,
        params={
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
        column_name="total_weight",
        db_column_name="total_weight",
        column_class_name="Real",
        column_class=Real,
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
        table_class_name="Deliveries",
        tablename="deliveries",
        column_name="is_custom_address",
        db_column_name="is_custom_address",
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
        column_name="latitude",
        db_column_name="latitude",
        column_class_name="Real",
        column_class=Real,
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
        table_class_name="Deliveries",
        tablename="deliveries",
        column_name="longitude",
        db_column_name="longitude",
        column_class_name="Real",
        column_class=Real,
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
        table_class_name="Deliveries",
        tablename="deliveries",
        column_name="created_by",
        db_column_name="created_by",
        column_class_name="BigInt",
        column_class=BigInt,
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
        table_class_name="Deliveries",
        tablename="deliveries",
        column_name="status",
        db_column_name="status",
        column_class_name="Varchar",
        column_class=Varchar,
        params={
            "length": 50,
            "default": "created",
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
