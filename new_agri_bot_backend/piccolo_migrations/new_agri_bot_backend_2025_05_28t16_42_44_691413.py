from piccolo.apps.migrations.auto.migration_manager import MigrationManager


ID = "2025-05-28T16:42:44:691413"
VERSION = "1.26.1"
DESCRIPTION = ""


async def forwards():
    manager = MigrationManager(
        migration_id=ID, app_name="", description=DESCRIPTION
    )

    def run():
        print(f"running {ID}")

    manager.add_raw(run)

    return manager
