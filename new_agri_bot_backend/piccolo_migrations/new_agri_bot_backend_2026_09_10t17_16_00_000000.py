from piccolo.apps.migrations.auto.migration_manager import MigrationManager


ID = "2026-09-10T17:16:00:000000"
VERSION = "1.26.1"
DESCRIPTION = "Mark old deliveries as is_received = True"


async def forwards():
    manager = MigrationManager(
        migration_id=ID, app_name="new_agri_bot_backend", description=DESCRIPTION
    )

    async def mark_old_deliveries_as_received():
        from piccolo.engine.finder import engine_finder
        engine = engine_finder()
        if engine:
            await engine.run_ddl(
                """
                UPDATE deliveries 
                SET is_received = TRUE 
                WHERE status = 'Виконано' 
                   OR delivery_date < CURRENT_DATE - INTERVAL '3 days'
                   OR (delivery_date IS NULL AND created_at < CURRENT_TIMESTAMP - INTERVAL '3 days');
                """
            )

    manager.add_raw(mark_old_deliveries_as_received)

    return manager
