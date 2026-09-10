from piccolo.apps.migrations.auto.migration_manager import MigrationManager
from piccolo.columns.column_types import BigInt, ForeignKey, OnDelete, OnUpdate
from piccolo.columns.indexes import IndexMethod
from piccolo.table import Table


class Users(Table, tablename="users", schema=None):
    telegram_id = BigInt(
        null=False,
        primary_key=True,
        unique=False,
        index=False,
        index_method=IndexMethod.btree,
        choices=None,
        db_column_name="telegram_id",
        secret=False,
    )


ID = "2026-09-10T11:15:00:000000"
VERSION = "1.26.1"
DESCRIPTION = "Change manager in ManagerAccountantGuide to ForeignKey referencing Users"


async def forwards():
    manager = MigrationManager(
        migration_id=ID, app_name="new_agri_bot_backend", description=DESCRIPTION
    )

    manager.drop_column(
        table_class_name="ManagerAccountantGuide",
        tablename="manager_accountant_guide",
        column_name="manager",
        db_column_name="manager",
    )

    manager.add_column(
        table_class_name="ManagerAccountantGuide",
        tablename="manager_accountant_guide",
        column_name="manager",
        db_column_name="manager",
        column_class_name="ForeignKey",
        column_class=ForeignKey,
        params={
            "references": Users,
            "on_delete": OnDelete.cascade,
            "on_update": OnUpdate.cascade,
            "target_column": None,
            "null": False,
            "primary_key": False,
            "unique": True,
            "index": True,
            "index_method": IndexMethod.btree,
            "choices": None,
            "db_column_name": None,
            "secret": False,
        },
        schema=None,
    )

    return manager
