from piccolo.apps.migrations.auto.migration_manager import MigrationManager
from piccolo.table import Table


ID = "2026-08-04T10:45:00:000000"
VERSION = "1.26.1"
DESCRIPTION = "Update details_for_orders VIEW to include shipping_warehouse"


async def forwards():
    manager = MigrationManager(
        migration_id=ID, app_name="new_agri_bot_backend", description=DESCRIPTION
    )

    async def run_queries(backwards=False):
        if not backwards:
            await manager._run_query(Table.raw("DROP VIEW IF EXISTS details_for_orders CASCADE"))

            await manager._run_query(Table.raw("""
                CREATE VIEW details_for_orders AS
                WITH s AS (
                        SELECT submissions.nomenclature,
                            submissions.party_sign,
                            submissions.buying_season,
                            submissions.different,
                            submissions.client,
                            submissions.contract_supplement,
                            submissions.manager,
                            submissions.shipping_warehouse,
                            submissions.product
                        FROM submissions
                        WHERE (submissions.different > (0)::double precision)
                        ), so AS (
                        SELECT submissions.product,
                            sum(submissions.different) AS orders_q
                        FROM submissions
                        WHERE ((submissions.different > (0)::double precision) AND ((submissions.document_status)::text = 'Ф'::text))
                        GROUP BY submissions.product
                        ), rs_total AS (
                        SELECT remains.product,
                            sum(remains.buh) AS buh,
                            sum(remains.skl) AS skl
                        FROM remains
                        GROUP BY remains.product
                        ), rs_party AS (
                        SELECT remains.product,
                            remains.nomenclature_series,
                            sum(remains.buh) AS buh,
                            sum(remains.skl) AS skl
                        FROM remains
                        GROUP BY remains.product, remains.nomenclature_series
                        )
                SELECT s.nomenclature,
                    s.party_sign,
                    s.buying_season,
                    s.different,
                    s.client,
                    s.contract_supplement,
                    s.manager,
                    s.shipping_warehouse,
                    s.product,
                    COALESCE((so.orders_q)::numeric, (0)::numeric) AS orders_q,
                    COALESCE((m.qt_moved)::numeric, (0)::numeric) AS moved_q,
                    COALESCE((m.party_sign)::character varying, ''::character varying) AS party,
                        CASE
                            WHEN (m.id IS NOT NULL) THEN COALESCE((rp.buh)::numeric, (0)::numeric)
                            ELSE COALESCE((rt.buh)::numeric, (0)::numeric)
                        END AS buh,
                        CASE
                            WHEN (m.id IS NOT NULL) THEN COALESCE((rp.skl)::numeric, (0)::numeric)
                            ELSE COALESCE((rt.skl)::numeric, (0)::numeric)
                        END AS skl,
                    uuid_generate_v4() AS id,
                        CASE
                            WHEN (m.id IS NOT NULL) THEN
                            CASE
                                WHEN ((COALESCE((rp.buh)::numeric, (0)::numeric) > (0)::numeric) AND (COALESCE((rp.skl)::numeric, (0)::numeric) > (0)::numeric)) THEN '2'::text
                                WHEN ((COALESCE((rp.buh)::numeric, (0)::numeric) > (0)::numeric) AND (COALESCE((rp.skl)::numeric, (0)::numeric) <= (0)::numeric)) THEN '1'::text
                                ELSE '0'::text
                            END
                            ELSE
                            CASE
                                WHEN ((COALESCE((rt.buh)::numeric, (0)::numeric) >= COALESCE((so.orders_q)::numeric, (0)::numeric)) AND (COALESCE((rt.skl)::numeric, (0)::numeric) >= COALESCE((so.orders_q)::numeric, (0)::numeric))) THEN '2'::text
                                WHEN ((COALESCE((rt.buh)::numeric, (0)::numeric) >= COALESCE((so.orders_q)::numeric, (0)::numeric)) AND (COALESCE((rt.skl)::numeric, (0)::numeric) < COALESCE((so.orders_q)::numeric, (0)::numeric))) THEN '1'::text
                                ELSE '0'::text
                            END
                        END AS qok
                FROM ((((s
                    LEFT JOIN so ON ((so.product = s.product)))
                    LEFT JOIN moved_data m ON ((((m.product_id)::text = (s.product)::text) AND ((m.contract)::text = (s.contract_supplement)::text) AND (m.is_active = true))))
                    LEFT JOIN rs_total rt ON ((rt.product = s.product)))
                    LEFT JOIN rs_party rp ON (((rp.product = s.product) AND ((rp.nomenclature_series)::text = (m.party_sign)::text))));
            """))

    manager.run = run_queries

    return manager
