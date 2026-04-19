from piccolo.apps.migrations.auto.migration_manager import MigrationManager
from piccolo.table import Table


ID = "2026-04-19T16:27:00:000000"
VERSION = "1.26.1"
DESCRIPTION = "Convert product_on_warehouse table to a VIEW and add parent_element support"


async def forwards():
    manager = MigrationManager(
        migration_id=ID, app_name="new_agri_bot_backend", description=DESCRIPTION
    )

    async def run_queries(backwards=False):
        if not backwards:
            # Drop the existing table or view if it exists
            await manager._run_query(Table.raw("DROP VIEW IF EXISTS product_on_warehouse CASCADE"))
            await manager._run_query(Table.raw("DROP TABLE IF EXISTS product_on_warehouse CASCADE"))
            
            # Create the view instead
            await manager._run_query(Table.raw("""
                CREATE VIEW product_on_warehouse AS
                SELECT DISTINCT ON (r.product)
                    (ROW_NUMBER() OVER (ORDER BY pg.product))::bigint AS id,
                    pg.product AS product,
                    r.line_of_business,
                    r.parent_element
                FROM remains r
                JOIN product_guide pg ON r.product = pg.id
                WHERE r.buh > 0;
            """))
        else:
            # For backwards, drop the view and let the previous migration recreate the table
            await manager._run_query(Table.raw("DROP VIEW IF EXISTS product_on_warehouse"))

    manager.run = run_queries

    return manager
