from piccolo.apps.migrations.auto.migration_manager import MigrationManager
from piccolo.columns.column_types import BigInt
from piccolo.columns.column_types import DoublePrecision
from piccolo.columns.column_types import ForeignKey
from piccolo.columns.column_types import Integer
from piccolo.columns.column_types import Varchar
from piccolo.table import Table


ID = "2026-03-11T16:06:42:635952"
VERSION = "1.25.0"
DESCRIPTION = ""


async def forwards():
    manager = MigrationManager(
        migration_id=ID, app_name="new_agri_bot_backend", description=DESCRIPTION
    )

    manager.alter_column(
        table_class_name="AvailableStock",
        tablename="available_stock",
        column_name="product",
        db_column_name="product",
        params={"index": True},
        old_params={"index": False},
        column_class=ForeignKey,
        old_column_class=ForeignKey,
        schema=None,
    )

    manager.alter_column(
        table_class_name="DeliveryItems",
        tablename="delivery_items",
        column_name="delivery",
        db_column_name="delivery",
        params={"index": True},
        old_params={"index": False},
        column_class=ForeignKey,
        old_column_class=ForeignKey,
        schema=None,
    )

    manager.alter_column(
        table_class_name="FreeStock",
        tablename="free_stock",
        column_name="product",
        db_column_name="product",
        params={"index": True},
        old_params={"index": False},
        column_class=ForeignKey,
        old_column_class=ForeignKey,
        schema=None,
    )

    manager.alter_column(
        table_class_name="MovedData",
        tablename="moved_data",
        column_name="qt_order",
        db_column_name="qt_order",
        params={"default": 0.0},
        old_params={"default": ""},
        column_class=DoublePrecision,
        old_column_class=Varchar,
        schema=None,
    )

    manager.alter_column(
        table_class_name="MovedData",
        tablename="moved_data",
        column_name="qt_moved",
        db_column_name="qt_moved",
        params={"default": 0.0},
        old_params={"default": ""},
        column_class=DoublePrecision,
        old_column_class=Varchar,
        schema=None,
    )

    manager.alter_column(
        table_class_name="MovedData",
        tablename="moved_data",
        column_name="product_id",
        db_column_name="product_id",
        params={"index": True},
        old_params={"index": False},
        column_class=Varchar,
        old_column_class=Varchar,
        schema=None,
    )

    manager.alter_column(
        table_class_name="OrderChatMessage",
        tablename="order_chat_message",
        column_name="order_ref",
        db_column_name="order_ref",
        params={"index": True},
        old_params={"index": False},
        column_class=Varchar,
        old_column_class=Varchar,
        schema=None,
    )

    manager.alter_column(
        table_class_name="OrderChatMessage",
        tablename="order_chat_message",
        column_name="user_id",
        db_column_name="user_id",
        params={"index": True},
        old_params={"index": False},
        column_class=BigInt,
        old_column_class=BigInt,
        schema=None,
    )

    manager.alter_column(
        table_class_name="OrderComments",
        tablename="order_comments",
        column_name="order_ref",
        db_column_name="order_ref",
        params={"index": True},
        old_params={"index": False},
        column_class=Varchar,
        old_column_class=Varchar,
        schema=None,
    )

    manager.alter_column(
        table_class_name="OrderComments",
        tablename="order_comments",
        column_name="created_by",
        db_column_name="created_by",
        params={},
        old_params={},
        column_class=BigInt,
        old_column_class=Integer,
        schema=None,
    )

    manager.alter_column(
        table_class_name="ProductUnderSubmissions",
        tablename="product_under_submissions",
        column_name="product",
        db_column_name="product",
        params={"index": True},
        old_params={"index": False},
        column_class=ForeignKey,
        old_column_class=ForeignKey,
        schema=None,
    )

    manager.alter_column(
        table_class_name="Remains",
        tablename="remains",
        column_name="product",
        db_column_name="product",
        params={"index": True},
        old_params={"index": False},
        column_class=ForeignKey,
        old_column_class=ForeignKey,
        schema=None,
    )

    manager.alter_column(
        table_class_name="Submissions",
        tablename="submissions",
        column_name="client",
        db_column_name="client",
        params={"index": True},
        old_params={"index": False},
        column_class=Varchar,
        old_column_class=Varchar,
        schema=None,
    )

    manager.alter_column(
        table_class_name="Submissions",
        tablename="submissions",
        column_name="contract_supplement",
        db_column_name="contract_supplement",
        params={"index": True},
        old_params={"index": False},
        column_class=Varchar,
        old_column_class=Varchar,
        schema=None,
    )

    manager.alter_column(
        table_class_name="Submissions",
        tablename="submissions",
        column_name="product",
        db_column_name="product",
        params={"index": True},
        old_params={"index": False},
        column_class=ForeignKey,
        old_column_class=ForeignKey,
        schema=None,
    )

    # Monkey patch the run method to handle view dependencies
    real_run = manager.run

    async def custom_run(backwards: bool = False):
        if not backwards:
            # 1. Drop views that depend on altered columns
            await manager._run_query(Table.raw("DROP VIEW IF EXISTS details_for_orders CASCADE"))
            await manager._run_query(Table.raw("DROP VIEW IF EXISTS moved_with_product_id CASCADE"))

        await real_run(backwards=backwards)

        if not backwards:
            # 2. Recreate views after column alterations
            # Recreate moved_with_product_id
            await manager._run_query(Table.raw("""
                CREATE VIEW moved_with_product_id AS
                SELECT m.product,
                    m.contract,
                    m.qt_moved,
                    m.party_sign,
                    p.id
                FROM (moved_data m
                    JOIN product_guide p ON (((p.product)::text = (m.product)::text)));
            """))

            # Recreate details_for_orders
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

    manager.run = custom_run

    return manager
