from piccolo.apps.migrations.auto.migration_manager import MigrationManager
from piccolo.table import Table


ID = "2026-04-19T16:32:00:000000"
VERSION = "1.26.1"
DESCRIPTION = "Update product_on_warehouse VIEW to use UUID IDs for compatibility"


async def forwards():
    manager = MigrationManager(
        migration_id=ID, app_name="new_agri_bot_backend", description=DESCRIPTION
    )

    async def run_queries(backwards=False):
        if not backwards:
            # Recreate the view with the correct UUID as ID
            await manager._run_query(Table.raw("DROP VIEW IF EXISTS product_on_warehouse CASCADE"))
            
            await manager._run_query(Table.raw("""
                CREATE VIEW product_on_warehouse AS
                SELECT DISTINCT ON (r.product)
                    pg.id AS id,
                    pg.product AS product,
                    r.line_of_business,
                    r.parent_element
                FROM remains r
                JOIN product_guide pg ON r.product = pg.id
                WHERE r.buh > 0;
            """))
        else:
            # Backwards would revert to the previous Row Number version (if needed)
            # but usually we don't go back.
            pass

    manager.run = run_queries

    return manager
