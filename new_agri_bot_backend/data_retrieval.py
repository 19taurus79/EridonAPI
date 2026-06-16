# app/data_retrieval.py
from datetime import datetime, timezone, timedelta
from typing import Optional, List
from collections import defaultdict
import io
from .cache import cached_endpoint

import pandas as pd
import requests
from fastapi import APIRouter, Query, HTTPException, status, Depends
from fastapi.responses import StreamingResponse
from piccolo.columns.defaults.timestamptz import TimestamptzNow
from piccolo.query import Sum
from pydantic import BaseModel, Field

from .google_calendar import (
    changed_color_calendar_events_by_id,
    changed_date_calendar_events_by_id,
    get_calendar_event_by_id,
    get_calendar_events,
)
from .config import bot, logger, SEND_NOTIFICATIONS

# from .main import get_calendar_events

# Імпортуйте ваші моделі Piccolo ORM
from .tables import (
    Remains,
    ProductGuide,
    Users,
    ClientManagerGuide,
    ClientAddress,
    ProductOnWarehouse,
    Submissions,
    AvailableStock,
    AvStockProd,
    MovedData,
    ProductsForOrders,
    DetailsForOrders,
    Tasks,
    Events,
    Payment,
    Deliveries,
    DeliveryItems,
    FreeStock,
)
from .telegram_auth import get_current_telegram_user, check_not_guest
from .tasks_handler import (
    get_all_tasks,
    create_task,
    get_task_by_id,
    complete_task,
    in_progress_task,
)
from .utils import extract_order_ref, format_delivery_final_data
from pydantic import BaseModel
from datetime import date


class ChangeDateRequest(BaseModel):
    new_date: date


router = APIRouter(
    prefix="/data",  # Всі едпоінти в цьому роутері починатимуться з /data
    tags=["Отримання даних"],  # Тег для Swagger UI
    # dependencies=[Depends(get_current_telegram_user)],
)


@router.get("/remains", summary="Отримати всі залишки на складі")
@cached_endpoint()
async def get_remains():
    """
    Повертає всі записи про залишки на складі з бази даних.
    """
    remains = await Remains.select().run()
    return remains


@router.get("/geocode")
def geocode(address: str = Query(..., description="Адрес для поиска")):
    url = "https://nominatim.openstreetmap.org/search"
    params = {"q": address, "format": "json", "addressdetails": "1", "layer": "address"}
    headers = {"User-Agent": "MyGeocodeApp/1.0"}  # укажи свой
    response = requests.get(url, params=params, headers=headers)
    return response.json()


@router.get("/remains/{product_id}", summary="Отримати залишки за конкретним продуктом")
@cached_endpoint()
async def get_remains_by_product(
    product_id: str,
):  # Використовуємо product_id для ясності
    """
    Повертає записи про залишки на складі для зазначеного продукту.
    Використовує `product_id` для фільтрації за полем `product`.
    """
    remains = await Remains.select().where(Remains.product == product_id).run()
    if not remains:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Залишки для продукту з ID '{product_id}' не знайдено.",
        )
    return remains


@router.get(
    "/remains_by_product", summary="Отримати залишки за конкретним продуктом"
)
@cached_endpoint()
async def get_remains_by_product(
    product: str = Query(..., description="Назва продукту"),
):

    product_id = (
        await ProductGuide.select(ProductGuide.id)
        .where(ProductGuide.product == product)
        .run()
    )
    remains = await Remains.select().where(Remains.product == product_id[0]["id"]).run()
    if not remains:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Залишки для продукту з ID '{product_id}' не знайдено.",
        )
    return remains


@router.get(
    "/remains_group/{product_id}",
    summary="Отримати залишки за конкретним продуктом, згруповані по партії ",
)
@cached_endpoint()
async def get_group_remains_by_product(product_id: str):
    remains = (
        await Remains.select(
            Remains.product.as_alias("product_id"), Sum(Remains.buh).as_alias("remains")
        )
        .where(Remains.product == product_id)
        .group_by(Remains.product)
        .run()
    )
    if not remains:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Залишки для продукту з ID '{product_id}' не знайдено.",
        )
    return remains


@router.get(
    "/av_stock/{product_id}",
    summary="Отримати вільні залишки на РУ за конкретним продуктом",
)
@cached_endpoint()
async def get_av_remains_by_product(
    product_id: str,
):  # Використовуємо product_id для ясності
    """
    Повертає записи про залишки на складі для зазначеного продукту.
    Використовує `product_id` для фільтрації за полем `product`.
    """
    remains = (
        await AvailableStock.select().where(AvailableStock.product == product_id).run()
    )
    if not remains:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Залишки для продукту з ID '{product_id}' не знайдено.",
        )
    
    # Отримуємо детальні залишки по складах з FreeStock
    free_stocks = await FreeStock.select().where(FreeStock.product == product_id).run()
    
    # Групуємо склади по підрозділах
    free_map = defaultdict(list)
    for fs in free_stocks:
        if fs["free_qty"] > 0:
            free_map[fs["division"]].append({
                "warehouse": fs["warehouse"],
                "available": fs["free_qty"]
            })
            
    # Додаємо інформацію про склади до кожного підрозділу
    for r in remains:
        div = r["division"]
        if div in free_map:
            r["warehouses"] = free_map[div]
        else:
            r["warehouses"] = [{
                "warehouse": r["division"],
                "available": r["available"]
            }]
            
    return remains


@router.get("/products", summary="Отримати список продуктів з можливістю пошуку")
@cached_endpoint()
async def get_products(category: Optional[str] = None, name_part: Optional[str] = None):
    """
    Повертає список всіх продуктів.
    Можна використовувати параметр `query` для пошуку продуктів за частиною назви (без урахування регістру).
    """
    query = AvStockProd.select()

    if category:
        query = query.where(AvStockProd.line_of_business == category)

    if name_part:
        # Використовуємо .ilike() для регістронезалежного пошуку по частині рядка
        # Якщо ваша ORM/БД не підтримує .ilike(), можливо, знадобиться інший підхід
        query = query.where(AvStockProd.product.ilike(f"%{name_part}%"))

    product = await query.run()
    return product


@router.get("/all_products")
@cached_endpoint()
async def get_all_product_by_guide(
    category: Optional[str] = None, 
    parent_category: Optional[str] = Query(None),
    name_part: Optional[str] = None
):
    query = ProductGuide.select()

    if category:
        query = query.where(ProductGuide.line_of_business == category)
        
    if parent_category:
        query = query.where(ProductGuide.parent_element == parent_category)

    if name_part:
        # Використовуємо .ilike() для регістронезалежного пошуку по частині рядка
        # Якщо ваша ORM/БД не підтримує .ilike(), можливо, знадобиться інший підхід
        query = query.where(ProductGuide.product.ilike(f"%{name_part}%"))

    product = await query.order_by(ProductGuide.product).run()
    return product


@router.get("/categories_tree")
@cached_endpoint()
async def get_categories_tree():
    """
    Повертає унікальні комбінації бізнес-напрямку та батьківського елемента (підгрупи)
    для побудови дерева фільтрів.
    """
    data = await ProductGuide.select(
        ProductGuide.line_of_business, 
        ProductGuide.parent_element
    ).distinct().run()
    return data


@router.get("/product/{product_id}", summary="Отримати інформацію про продукт за ID")
@cached_endpoint()
async def get_product_by_id(
    product_id: str,
):  # Припускаємо, що product_id є цілочисельним первинним ключем
    """
    Повертає інформацію про продукт за його унікальним ID.
    """
    product = (
        await ProductGuide.objects().where(ProductGuide.id == product_id).first().run()
    )
    if not product:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Продукт з ID '{product_id}' не знайдено.",
        )
    return product


@router.get("/managers")
@cached_endpoint()
async def get_managers():
    managers = (
        await Submissions.select(Submissions.manager)
        .distinct()
        .order_by(Submissions.manager)
        .run()
    )
    # Очищаем от пустых значений и пробелов
    unique_managers = sorted({r["manager"].strip() for r in managers if r.get("manager") and r["manager"].strip()})
    return [{"manager": m} for m in unique_managers]


@router.get(
    "/clients",
    summary="отримати клієнтів по менеджеру, якщо адмін то усіх ",
    dependencies=[Depends(get_current_telegram_user)],
)
@cached_endpoint()
async def get_clients(
    manager: dict = Depends(get_current_telegram_user), name_part: Optional[str] = None
):
    if manager["is_admin"]:
        query = ClientManagerGuide.select()
    else:
        query = ClientManagerGuide.select().where(
            ClientManagerGuide.manager == manager["full_name_for_orders"]
        )
    if name_part:
        query = query.where(ClientManagerGuide.client.ilike(f"%{name_part}%"))
    clients = await query.run()
    return clients


@router.get(
    "/product_on_warehouse",
    summary="Отримати товари, по яким є залишки на складі, з опціональними фільтрами",
    dependencies=[Depends(get_current_telegram_user)],
)
@cached_endpoint()
async def get_product_on_warehouse(
    category: Optional[str] = None, 
    parent_category: Optional[str] = Query(None),
    name_part: Optional[str] = None,
    free_only: bool = Query(False)
):
    """
    Повертає записи про товари, по яким є залишки на складі, з бази даних.
    Опціональні фільтри:
    - `category`: Фільтрувати за назвою категории (повна відповідність).
    - `name_part`: Фільтрувати за частиною найменування товару (нечутливий до регістру).
    - `free_only`: Фільтрувати тільки товари з вільним залишком.
    """
    query = ProductOnWarehouse.select()

    if category:
        query = query.where(ProductOnWarehouse.line_of_business == category)

    if parent_category:
        query = query.where(ProductOnWarehouse.parent_element == parent_category)

    if name_part:
        # Використовуємо .ilike() для регістронезалежного пошуку по частині рядка
        # Якщо ваша ORM/БД не підтримує .ilike(), можливо, знадобиться інший підхід
        query = query.where(ProductOnWarehouse.product.ilike(f"%{name_part}%"))

    if free_only:
        free_ids_query = """
            SELECT pg.id
            FROM remains r
            JOIN product_guide pg ON r.product = pg.id
            LEFT JOIN (
                SELECT product, 
                       COALESCE(SUM(CASE WHEN document_status = 'затверджено' THEN different ELSE 0 END), 0) as orders_q,
                       COALESCE(SUM(CASE WHEN document_status IN ('затверджено', 'продукція затверджена') THEN different ELSE 0 END), 0) as total_orders
                FROM submissions
                WHERE different > 0
                GROUP BY product
            ) o ON o.product = pg.id
            GROUP BY pg.id, o.orders_q, o.total_orders
            HAVING SUM(r.buh) > 0 AND ((SUM(r.buh) - COALESCE(o.orders_q, 0) > 0) OR (COALESCE(o.total_orders, 0) = 0))
        """
        raw_result = await ProductOnWarehouse.raw(free_ids_query)
        free_ids = [row["id"] for row in raw_result]
        query = query.where(ProductOnWarehouse.id.is_in(free_ids))

    product = await query.order_by(ProductOnWarehouse.product).run()
    return product


@router.get(
    "/product_on_warehouse/export",
    summary="Експортувати залишки на складі в Excel",
    dependencies=[Depends(get_current_telegram_user)],
)
async def export_product_on_warehouse(
    category: Optional[str] = None, 
    parent_category: Optional[str] = Query(None),
    name_part: Optional[str] = None,
    free_only: bool = Query(False),
    columns: Optional[str] = Query(None)
):
    """
    Формує та повертає Excel-файл з поточними зашликами та вільними залишками товарів,
    враховуючи всі застосовані фільтри та обрані стовпці.
    """
    query = ProductOnWarehouse.select()

    if category:
        query = query.where(ProductOnWarehouse.line_of_business == category)

    if parent_category:
        query = query.where(ProductOnWarehouse.parent_element == parent_category)

    if name_part:
        query = query.where(ProductOnWarehouse.product.ilike(f"%{name_part}%"))

    if free_only:
        free_ids_query = """
            SELECT pg.id
            FROM remains r
            JOIN product_guide pg ON r.product = pg.id
            LEFT JOIN (
                SELECT product, 
                       COALESCE(SUM(CASE WHEN document_status = 'затверджено' THEN different ELSE 0 END), 0) as orders_q,
                       COALESCE(SUM(CASE WHEN document_status IN ('затверджено', 'продукція затверджена') THEN different ELSE 0 END), 0) as total_orders
                FROM submissions
                WHERE different > 0
                GROUP BY product
            ) o ON o.product = pg.id
            GROUP BY pg.id, o.orders_q, o.total_orders
            HAVING SUM(r.buh) > 0 AND ((SUM(r.buh) - COALESCE(o.orders_q, 0) > 0) OR (COALESCE(o.total_orders, 0) = 0))
        """
        raw_result = await ProductOnWarehouse.raw(free_ids_query)
        free_ids = [row["id"] for row in raw_result]
        query = query.where(ProductOnWarehouse.id.is_in(free_ids))

    products = await query.order_by(ProductOnWarehouse.product).run()

    if not products:
        df = pd.DataFrame(columns=[
            "Товар", 
            "Напрямок діяльності", 
            "Підгрупа", 
            "Партія",
            "Склад",
            "Бухгалтерський залишок", 
            "Складський залишок", 
            "На збереганні", 
            "Заявки (всього по товару)", 
            "Вільний залишок (всього по товару)",
            "Рік врожаю",
            "Схожість",
            "МТН",
            "Країна походження",
            "Активна речовина",
            "Сертифікат"
        ])
    else:
        product_ids_str = ", ".join([f"'{p['id']}'" for p in products])
        sql = f"""
            SELECT 
                pg.product AS "Товар",
                r.line_of_business AS "Напрямок діяльності",
                r.parent_element AS "Підгрупа",
                r.nomenclature_series AS "Партія",
                r.warehouse AS "Склад",
                SUM(r.buh) AS "Бухгалтерський залишок",
                SUM(r.skl) AS "Складський залишок",
                SUM(r.storage) AS "На збереганні",
                COALESCE(o.orders_q, 0) AS "Заявки (всього по товару)",
                (p_tot.total_buh - COALESCE(o.orders_q, 0)) AS "Вільний залишок (всього по товару)",
                MAX(r.crop_year) AS "Рік врожаю",
                MAX(r.germination) AS "Схожість",
                MAX(r.mtn) AS "МТН",
                MAX(r.origin_country) AS "Країна походження",
                MAX(r.active_substance) AS "Активна речовина",
                MAX(r.certificate) AS "Сертифікат"
            FROM remains r
            JOIN product_guide pg ON r.product = pg.id
            JOIN (
                SELECT product, SUM(buh) as total_buh
                FROM remains
                GROUP BY product
            ) p_tot ON p_tot.product = pg.id
            LEFT JOIN (
                SELECT product, 
                       COALESCE(SUM(CASE WHEN document_status = 'затверджено' THEN different ELSE 0 END), 0) as orders_q
                FROM submissions
                WHERE different > 0
                GROUP BY product
            ) o ON o.product = pg.id
            WHERE pg.id IN ({product_ids_str})
            GROUP BY pg.product, r.line_of_business, r.parent_element, r.nomenclature_series, r.warehouse, o.orders_q, p_tot.total_buh
            ORDER BY pg.product, r.nomenclature_series, r.warehouse
        """
        raw_results = await Remains.raw(sql)
        df = pd.DataFrame(raw_results)

    if columns:
        cols_to_keep = [c.strip() for c in columns.split(",") if c.strip()]
        valid_cols = [c for c in cols_to_keep if c in df.columns]
        if valid_cols:
            df = df[valid_cols]

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name="Залишки")
    
    output.seek(0)
    
    headers = {
        'Content-Disposition': 'attachment; filename="remains.xlsx"'
    }
    
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers
    )


@router.get("/orders")
@cached_endpoint()
async def get_orders(client: str = Query(...)):
    orders = (
        await Submissions.select()
        .where((Submissions.client == client) & (Submissions.different > 0))
        .run()
    )
    return orders


@router.get("/contracts")
@cached_endpoint()
async def get_contracts(client: str = Query(...)):
    client_from_guide = await ClientManagerGuide.select(
        ClientManagerGuide.client
    ).where(ClientManagerGuide.id == int(client))
    
    if not client_from_guide:
        return []

    contracts = (
        await Submissions.select(
            Submissions.contract_supplement,
            Submissions.line_of_business,
            Submissions.document_status,
            Submissions.delivery_status,
        )
        .where(
            (Submissions.different > 0)
            & (Submissions.client == client_from_guide[0]["client"])
        )
        .group_by(
            Submissions.contract_supplement,
            Submissions.line_of_business,
            Submissions.document_status,
            Submissions.delivery_status,
        )
        .order_by(Submissions.contract_supplement)
        .run()
    )

    if contracts:
        # Збираємо номери доповнень
        contract_ids = [item["contract_supplement"] for item in contracts if item.get("contract_supplement")]
        
        if contract_ids:
            # Отримуємо дані про оплату
            payments = await Payment.select(
                Payment.contract_supplement,
                Payment.contract_type,
                Payment.loan_percentage,
                Payment.planned_amount,
                Payment.actual_payment_amount
            ).where(Payment.contract_supplement.is_in(contract_ids)).run()
            
            payment_map = {p["contract_supplement"]: p for p in payments}
            
            # Збагачуємо дані контрактів
            for item in contracts:
                cs = item.get("contract_supplement")
                p_info = payment_map.get(cs)
                if p_info:
                    item["contract_type"] = p_info["contract_type"]
                    item["loan_percentage"] = p_info["loan_percentage"]
                    item["planned_amount"] = p_info["planned_amount"]
                    item["actual_payment_amount"] = p_info["actual_payment_amount"]
                else:
                    item["contract_type"] = None
                    item["loan_percentage"] = None
                    item["planned_amount"] = None
                    item["actual_payment_amount"] = None

    return contracts


@router.get("/contract_detail/{contract}")
@cached_endpoint()
async def get_contract_detail(contract):
    detail = (
        await Submissions.select(
            Submissions.nomenclature,
            Submissions.party_sign,
            Submissions.buying_season,
            Submissions.different,
            Submissions.client,
            Submissions.contract_supplement,
            Submissions.manager,
            Submissions.product,
        )
        .where(
            (Submissions.contract_supplement == contract) & (Submissions.different > 0)
        )
        .run()
    )
    return detail


@router.get("/sum_order_by_product")
@cached_endpoint()
async def get_sum_order_products(product: str = Query(...)):
    total_sum = (
        await Submissions.select(
            Submissions.product.as_alias("product_id"),
            Sum(Submissions.different).as_alias("total_orders"),
        )
        .where(
            (Submissions.product == product)
            & (Submissions.different > 0)
            & (Submissions.document_status == "затверджено")
        )
        .group_by(Submissions.product)
        .run()
    )
    if total_sum == []:
        total_sum = [{"product_id": product, "total_orders": 0}]
    return total_sum


@router.get("/sum_orders_tiers_by_product", summary="Потреба по двох рівнях пріоритету")
@cached_endpoint()
async def get_sum_orders_tiers_by_product(product: str = Query(...)):
    """
    Повертає сумарну потребу по товару у двох рівнях:
    - Tier 1 (orders_q):                   статус 'затверджено'
    - Tier 2 (orders_q_product_confirmed): статус 'продукція затверджена'
    - orders_q_total:                      сума обох рівнів
    """
    # Tier 1
    t1 = (
        await Submissions.select(Sum(Submissions.different).as_alias("q"))
        .where(
            (Submissions.product == product)
            & (Submissions.different > 0)
            & (Submissions.document_status == "затверджено")
        )
        .run()
    )
    # Tier 2
    t2 = (
        await Submissions.select(Sum(Submissions.different).as_alias("q"))
        .where(
            (Submissions.product == product)
            & (Submissions.different > 0)
            & (Submissions.document_status == "продукція затверджена")
        )
        .run()
    )
    orders_q = float(t1[0]["q"] or 0) if t1 else 0.0
    orders_q_product_confirmed = float(t2[0]["q"] or 0) if t2 else 0.0
    return {
        "product_id": product,
        "orders_q": orders_q,
        "orders_q_product_confirmed": orders_q_product_confirmed,
        "orders_q_total": orders_q + orders_q_product_confirmed,
    }


@router.get("/order_by_product")
@cached_endpoint()
async def get_orders_by_product(product: str = Query(...)):
    data = (
        await Submissions.select()
        .where(
            (Submissions.product == product)
            & (Submissions.different > 0)
            & (Submissions.document_status.is_in(["затверджено", "продукція затверджена"]))
        )
        .run()
    )
    
    if not data:
        return []

    # Збираємо номери доповнень
    contract_ids = [item["contract_supplement"] for item in data if item.get("contract_supplement")]
    
    if contract_ids:
        # Отримуємо дані про оплату
        payments = await Payment.select(
            Payment.contract_supplement,
            Payment.contract_type,
            Payment.loan_percentage,
            Payment.planned_amount,
            Payment.actual_payment_amount
        ).where(Payment.contract_supplement.is_in(contract_ids)).run()
        
        payment_map = {p["contract_supplement"]: p for p in payments}
        
        # Збагачуємо дані замовлень
        for item in data:
            cs = item.get("contract_supplement")
            p_info = payment_map.get(cs)
            if p_info:
                item["contract_type"] = p_info["contract_type"]
                item["loan_percentage"] = p_info["loan_percentage"]
                item["planned_amount"] = p_info["planned_amount"]
                item["actual_payment_amount"] = p_info["actual_payment_amount"]
            else:
                item["contract_type"] = None
                item["loan_percentage"] = None
                item["planned_amount"] = None
                item["actual_payment_amount"] = None

    return data


@router.get("/moved_products_for_order/{order}")
@cached_endpoint()
async def get_moved_products_for_order(order: str):
    data = await MovedData.select().where(
        (MovedData.contract == order) & (MovedData.is_active == True)
    )
    return data


@router.get("/products_for_all_orders")
@cached_endpoint()
async def get_products_for_all_orders():
    data = await ProductsForOrders.select().run()
    return data


@router.get("/party_data")
@cached_endpoint()
async def get_party_data(
    id: Optional[str] = Query(None, description="Унікальний ID партії в базі даних"),
    party: Optional[str] = Query(None, description="Номер серії номенклатури (партія)"),
):
    """
    Отримує дані по партії. Пошук можливий або по ID, або по номеру партії.
    Якщо вказано обидва параметри, пріоритет надається ID.
    """
    if not id and not party:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Необхідно вказати або 'id', або 'party' для пошуку.",
        )

    query = Remains.select(
        Remains.crop_year,
        Remains.germination,
        Remains.mtn,
        Remains.origin_country,
        Remains.weight,
    )

    if id:
        query = query.where(Remains.id == id)
    elif party:
        query = query.where(Remains.nomenclature_series == party)

    data = await query.run()
    if not data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Дані по партії не знайдено."
        )
    return data


@router.get("/id_in_remains")
@cached_endpoint()
async def get_id_in_remains(party: str):
    data = (
        await Remains.select(Remains.id)
        .where(Remains.nomenclature_series == party)
        .run()
    )
    return data


def group_products_with_parties(items):
    grouped = {}

    for item in items:
        product_uuid = item["product"]
        contract_supplement = item.get("contract_supplement")
        
        # Ключ групування: товар + доповнення (замовлення)
        # Це важливо при пакетному запиті декількох замовлень
        group_key = (product_uuid, contract_supplement)

        if group_key not in grouped:
            grouped[group_key] = {
                "id": str(item.get("id")) if item.get("id") else None,
                "nomenclature": item.get("nomenclature"),
                "party_sign": item.get("party_sign"),
                "buying_season": item.get("buying_season"),
                "different": float(item.get("different") or 0.0), # Беремо один раз
                "client": item.get("client"),
                "contract_supplement": contract_supplement,
                "manager": item.get("manager"),
                "product": str(product_uuid),
                "orders_q": 0.0,
                "buh": 0.0,
                "skl": 0.0,
                "qok": str(item.get("qok")),
                "parties": [],
            }

        group = grouped[group_key]

        # Сумуємо тільки вільні та бухоблікові показники (вони потім перезаписуються, але для чистоти)
        # А головне - ми НЕ сумуємо 'different' тут!
        ord_val = item.get("orders_q")
        buh_val = item.get("buh")
        skl_val = item.get("skl")
        
        group["orders_q"] += float(ord_val) if ord_val is not None else 0.0
        group["buh"] += float(buh_val) if buh_val is not None else 0.0
        group["skl"] += float(skl_val) if skl_val is not None else 0.0

        # Додаємо партію
        party_data = {
            "moved_q": (
                float(item.get("moved_q")) if item.get("moved_q") is not None else 0
            ),
            "party": item.get("party"),
        }
        group["parties"].append(party_data)

    return list(grouped.values())


@router.post("/details_for_orders/batch")
@cached_endpoint()
async def get_details_for_orders_batch(order_list: List[str]):
    """
    Пакетне отримання деталей замовлень через POST (для обходу лімітів URL).
    """
    if not order_list:
        return []

    data = await DetailsForOrders.select().where(
        DetailsForOrders.contract_supplement.is_in(order_list)
    )
    result = group_products_with_parties(data)
    
    # Решта логіки така ж, як у GET версії
    return await _process_details_result(result)


async def _process_details_result(result):
    product_ids = [item["product"] for item in result if item.get("product")]
    if not product_ids:
        return result

    # 1. Оновлюємо бухоблікові залишки (сума по всім складам і партіям)
    remains_totals = (
        await Remains.select(
            Remains.product.as_alias("product_id"),
            Sum(Remains.buh).as_alias("total_buh"),
            Sum(Remains.skl).as_alias("total_skl"),
        )
        .where(Remains.product.is_in(product_ids))
        .group_by(Remains.product)
        .run()
    )
    totals_map = {str(r["product_id"]): r for r in remains_totals}

    # 2. TIER 1: Потреба по статусу "затверджено" — основний попит
    zatverdzeno_data = (
        await Submissions.select(
            Submissions.product.as_alias("product_id"),
            Sum(Submissions.different).as_alias("total_demand")
        )
        .where(
            (Submissions.product.is_in(product_ids)) &
            (Submissions.document_status == "затверджено") &
            (Submissions.different > 0)
        )
        .group_by(Submissions.product)
        .run()
    )
    zatverdzeno_map = {
        str(s["product_id"]): float(s["total_demand"] or 0)
        for s in zatverdzeno_data
    }

    # 3. TIER 2: Потреба по статусу "продукція затверджена" — додатковий попит
    product_confirmed_data = (
        await Submissions.select(
            Submissions.product.as_alias("product_id"),
            Sum(Submissions.different).as_alias("total_demand")
        )
        .where(
            (Submissions.product.is_in(product_ids)) &
            (Submissions.document_status == "продукція затверджена") &
            (Submissions.different > 0)
        )
        .group_by(Submissions.product)
        .run()
    )
    product_confirmed_map = {
        str(s["product_id"]): float(s["total_demand"] or 0)
        for s in product_confirmed_data
    }

    # 4. Перевіряємо наявність чернеток: "створено менеджером", "до розгляду", "розглядається"
    draft_statuses = ["створено менеджером", "до розгляду", "розглядається"]
    drafts_data = (
        await Submissions.select(
            Submissions.product.as_alias("product_id"),
            Submissions.contract_supplement.as_alias("contract")
        )
        .where(
            (Submissions.product.is_in(product_ids)) &
            (Submissions.document_status.is_in(draft_statuses)) &
            (Submissions.different > 0)
        )
        .run()
    )
    draft_pairs = {
        (str(d["contract"]), str(d["product_id"]))
        for d in drafts_data
    }

    # 5. Отримуємо дані про оплату для всіх доповнень у списку
    contract_ids = list(set(item["contract_supplement"] for item in result if item.get("contract_supplement")))
    payment_map = {}
    status_map = {}
    if contract_ids:
        # Отримуємо дані про оплату
        payments = await Payment.select(
            Payment.contract_supplement,
            Payment.contract_type,
            Payment.loan_percentage,
            Payment.planned_amount,
            Payment.actual_payment_amount
        ).where(Payment.contract_supplement.is_in(contract_ids)).run()
        payment_map = {p["contract_supplement"]: p for p in payments}

        # Отримуємо статуси з таблиці Submissions
        status_data = await Submissions.select(
            Submissions.contract_supplement,
            Submissions.document_status,
            Submissions.delivery_status
        ).where(Submissions.contract_supplement.is_in(contract_ids)).distinct().run()
        status_map = {s["contract_supplement"]: s for s in status_data}

    # 6. Перезаписуємо значення в результаті
    for item in result:
        pid = str(item.get("product", ""))
        contract = str(item.get("contract_supplement", ""))

        if pid in totals_map:
            item["buh"] = float(totals_map[pid]["total_buh"] or 0)
            item["skl"] = float(totals_map[pid]["total_skl"] or 0)

        # Tier 1: "затверджено" — основний попит
        item["orders_q"] = zatverdzeno_map.get(pid, 0.0)

        # Tier 2: "продукція затверджена" — додатковий попит
        item["orders_q_product_confirmed"] = product_confirmed_map.get(pid, 0.0)

        # Додаємо інформацію про оплату
        p_info = payment_map.get(item.get("contract_supplement"))
        if p_info:
            item["contract_type"] = p_info["contract_type"]
            item["loan_percentage"] = p_info["loan_percentage"]
            item["planned_amount"] = p_info["planned_amount"]
            item["actual_payment_amount"] = p_info["actual_payment_amount"]
        else:
            item["contract_type"] = None
            item["loan_percentage"] = None
            item["planned_amount"] = None
            item["actual_payment_amount"] = None

        item["has_draft"] = (contract, pid) in draft_pairs
        
        # Додаємо статуси з мапи
        s_info = status_map.get(item.get("contract_supplement"))
        if s_info:
            item["document_status"] = s_info["document_status"]
            item["delivery_status"] = s_info["delivery_status"]
        else:
            item["document_status"] = None
            item["delivery_status"] = None
        
        # Загальна потреба (зручно для quick-check)
        item["orders_q_total"] = item["orders_q"] + item["orders_q_product_confirmed"]

        # Чи є чернетки по цій заявці (для індикатора)
        item["has_draft"] = (contract, pid) in draft_pairs

    return result



@router.get("/details_for_orders/{order}")
@cached_endpoint()
async def get_details_for_order(order: str):
    # Підтримка списку замовлень через кому: "ID1,ID2,ID3"
    order_list = [o.strip() for o in order.split(",") if o.strip()]
    
    if not order_list:
        return []

    data = await DetailsForOrders.select().where(
        DetailsForOrders.contract_supplement.is_in(order_list)
    )
    result = group_products_with_parties(data)

    return await _process_details_result(result)


def clean_df_encoding(df: pd.DataFrame) -> pd.DataFrame:
    """
    Проходится по всем текстовым колонкам DataFrame и исправляет ошибки кодировки,
    которые могут вызывать UnicodeDecodeError при сериализации в JSON.
    """
    # Создаем копию, чтобы избежать предупреждения SettingWithCopyWarning
    df_copy = df.copy()
    # Выбираем только колонки с текстовыми данными
    for col in df_copy.select_dtypes(include=["object"]).columns:
        # Применяем "магическую" формулу для исправления кодировки
        df_copy[col] = df_copy[col].apply(
            lambda x: (
                x.encode("latin1", "ignore").decode("utf-8", "ignore")
                if isinstance(x, str)
                else x
            )
        )
    return df_copy


@router.get("/moved_products")
@cached_endpoint()
async def get_moved_products(product_id: str = Query(...)):
    orders = (
        await Submissions.select()
        .where((Submissions.product == product_id) & (Submissions.different > 0))
        .run()
    )
    valid_sub = []
    for order in orders:
        valid_sub.append(order["contract_supplement"])
    if valid_sub:
        moved = (
            await MovedData.select()
            .where(
                (MovedData.product_id == product_id)
                & (MovedData.contract.is_in(valid_sub))
                & (MovedData.is_active == True)
            )
            .run()
        )
    else:
        return []
    if moved:
        orders_df = pd.DataFrame(orders)
        moved_df = pd.DataFrame(moved)
        moved_df["qt_moved"] = moved_df["qt_moved"].astype(float)
        grouped_moved_df = (
            moved_df.groupby(["contract", "product", "party_sign"])["qt_moved"]
            .sum()
            .reset_index()
        )
        response_df = pd.merge(
            orders_df,
            grouped_moved_df,
            left_on="contract_supplement",
            right_on="contract",
        )
        # cleaned_df = clean_df_encoding(response_df)
        df_col = [
            "manager",
            "client",
            "contract",
            "party_sign_y",
            "product_y",
            "qt_moved",
            "different",
        ]
        # json_output = response_df[df_col].to_json(
        #     orient="records", indent=4, force_ascii=False
        # )
        dict_df = response_df[df_col].to_dict(orient="records")
        return dict_df
    else:
        return []


@router.get("/calendar_events")
def get_events_route(start: Optional[str] = None, end: Optional[str] = None):
    data = get_calendar_events(start_date=start, end_date=end)
    return data


@router.get("/calendar_event_by_id")
async def get_calendar_event_by_id_route(id: str):
    data = get_calendar_event_by_id(id)
    return data


@router.get("/calendar_events_by_user")
async def get_events_by_user(user=Depends(get_current_telegram_user)):
    three_days_ago = datetime.now() - timedelta(days=3)
    if user.is_admin:
        data = (
            await Events.select()
            .where((Events.start_event >= three_days_ago) | (Events.event_status != 2))
            .order_by(Events.start_event)
        )
    else:
        data = (
            await Events.select()
            .where(
                (Events.event_creator == user.telegram_id)
                & ((Events.start_event >= three_days_ago) | (Events.event_status != 2))
            )
            .order_by(Events.start_event)
        )
    return data


@router.get("/get_all_tasks")
async def get_tasks(user=Depends(get_current_telegram_user)):
    data = await get_all_tasks(user)
    return data


class TaskCreate(BaseModel):
    date: Optional[datetime] = Field(default_factory=lambda: datetime.utcnow())
    title: str
    note: str


@router.post("/add_task", dependencies=[Depends(check_not_guest)])
async def add_task(task_data: TaskCreate, user=Depends(get_current_telegram_user)):
    date_iso = (
        task_data.date.astimezone(timezone.utc).isoformat()
        if task_data.date
        else datetime.now(timezone.utc).isoformat()
    )
    task = await create_task(
        date=date_iso, note=task_data.note, title=task_data.title, user=user
    )
    return task


@router.get("/get_task")
def get_task(task_id):
    task = get_task_by_id(task_id)
    return task


@router.get("/get_delivery_by_task")
async def get_delivery_by_task(task_id: str):
    logger.info(f"🔍 get_delivery_by_task called with task_id: {task_id}")
    task = get_task_by_id(task_id)
    if not task:
        logger.warning(f"❌ Task {task_id} not found in Google Tasks")
        return {"found": False, "message": "Task not found in Google Tasks"}
    
    title = task.get("title", "")
    notes = task.get("notes", "")
    logger.info(f"📝 Task details fetched. Title: {title}, Notes length: {len(notes) if notes else 0}")
    
    order_ref = extract_order_ref(title) or extract_order_ref(notes)
    logger.info(f"🔎 Extracted order_ref: {order_ref}")
    if not order_ref:
        return {"found": False, "message": "No order reference found in task", "order_ref": None}
    
    # Query DeliveryItems by order_ref
    items = await DeliveryItems.select().where(DeliveryItems.order_ref == order_ref).run()
    logger.info(f"📦 Found {len(items)} DeliveryItems matching {order_ref}")
    if not items:
        return {"found": False, "message": f"No delivery items found for order {order_ref}", "order_ref": order_ref}
    
    delivery_id = items[0]["delivery"]
    logger.info(f"🚚 Associated delivery ID: {delivery_id}")
    
    # Fetch the full delivery details
    delivery = await Deliveries.objects().where(Deliveries.id == delivery_id).first().run()
    if not delivery:
        logger.warning(f"❌ Delivery {delivery_id} not found in DB")
        return {"found": False, "message": "Delivery not found", "order_ref": order_ref}
        
    # Fetch all items belonging to this delivery
    all_delivery_items = await DeliveryItems.select().where(DeliveryItems.delivery == delivery_id).run()
    logger.info(f"✅ Success! Returning delivery details and {len(all_delivery_items)} items")
    
    return {
        "found": True,
        "order_ref": order_ref,
        "delivery": {
            "id": delivery.id,
            "client": delivery.client,
            "manager": delivery.manager,
            "address": delivery.address,
            "contact": delivery.contact,
            "phone": delivery.phone,
            "delivery_date": str(delivery.delivery_date) if delivery.delivery_date else None,
            "comment": delivery.comment,
            "total_weight": delivery.total_weight,
            "status": delivery.status,
        },
        "items": all_delivery_items
    }


@router.get("/get_delivery_by_event")
async def get_delivery_by_event(event_id: str):
    logger.info(f"🔍 get_delivery_by_event called with event_id: {event_id}")
    delivery = await Deliveries.objects().where(Deliveries.calendar_id == event_id).first().run()
    if not delivery:
        logger.warning(f"❌ No delivery found for event_id {event_id}")
        return {"found": False, "message": "Delivery not found"}
        
    # Fetch all items belonging to this delivery
    all_delivery_items = await DeliveryItems.select().where(DeliveryItems.delivery == delivery.id).run()
    logger.info(f"✅ Success! Returning delivery details and {len(all_delivery_items)} items for event {event_id}")
    
    return {
        "found": True,
        "delivery": {
            "id": delivery.id,
            "client": delivery.client,
            "manager": delivery.manager,
            "address": delivery.address,
            "contact": delivery.contact,
            "phone": delivery.phone,
            "delivery_date": str(delivery.delivery_date) if delivery.delivery_date else None,
            "comment": delivery.comment,
            "total_weight": delivery.total_weight,
            "status": delivery.status,
        },
        "items": all_delivery_items
    }


@router.patch("/task_in_progress", dependencies=[Depends(check_not_guest)])
async def task_in_progress(task_id, user=Depends(get_current_telegram_user)):
    await Tasks.update(
        {
            Tasks.task_status: 1,
            Tasks.task_who_changed_id: user.telegram_id,
            Tasks.task_who_changed_name: user.full_name_for_orders,
        },
        force=True,
    ).where(Tasks.task_id == task_id).run()
    in_progress_task(task_id, user)

    # Відправляємо сповіщення автору задачі про взяття в роботу
    try:
        task_data = await Tasks.select().where(Tasks.task_id == task_id).run()
        if task_data:
            creator_id = task_data[0]["task_creator"]
            task_title = task_data[0]["task"]
            
            # Спробуємо підтягнути фактичні дані по доставці
            task_details = get_task_by_id(task_id)
            final_data_text = ""
            if task_details:
                notes = task_details.get("notes", "")
                order_ref = extract_order_ref(task_title) or extract_order_ref(notes)
                if order_ref:
                    items = await DeliveryItems.select().where(DeliveryItems.order_ref == order_ref).run()
                    if items:
                        delivery_id = items[0]["delivery"]
                        final_data_text = "\n\n" + await format_delivery_final_data(delivery_id)
            
            notification_text = (
                f"🔄 <b>Завдання взято в роботу</b>\n\n"
                f"📋 <b>Задача:</b> {task_title}\n"
                f"👷 <b>Виконавець:</b> {user.full_name_for_orders}"
                f"{final_data_text}"
            )
            if SEND_NOTIFICATIONS:
                await bot.send_message(
                    chat_id=creator_id,
                    text=notification_text,
                    parse_mode="HTML",
                )
            else:
                logger.info(f"🔇 Сповіщення вимкнено. Не надсилаємо старт задачі {task_id} автору {creator_id}")
    except Exception as e:
        logger.error(f"Помилка відправки сповіщення про взяття задачі в роботу: {e}")


class TaskComplete(BaseModel):
    tasks_status: int
    solution: str


@router.patch("/task_completed", dependencies=[Depends(check_not_guest)])
async def task_completed(task_id: str, body: TaskComplete, user=Depends(get_current_telegram_user)):
    await Tasks.update(
        {
            Tasks.task_status: 2,
            Tasks.task_who_changed_id: user.telegram_id,
            Tasks.task_who_changed_name: user.full_name_for_orders,
        },
        force=True,
    ).where(Tasks.task_id == task_id).run()
    complete_task(task_id, user)

    # Відправляємо сповіщення автору задачі
    try:
        task_data = await Tasks.select().where(Tasks.task_id == task_id).run()
        if task_data:
            creator_id = task_data[0]["task_creator"]
            task_title = task_data[0]["task"]
            notification_text = (
                f"✅ <b>Завдання виконано!</b>\n\n"
                f"📋 <b>Задача:</b> {task_title}\n"
                f"👷 <b>Виконавець:</b> {user.full_name_for_orders}\n"
                f"💬 <b>Рішення:</b> {body.solution}"
            )
            if SEND_NOTIFICATIONS:
                await bot.send_message(
                    chat_id=creator_id,
                    text=notification_text,
                    parse_mode="HTML",
                )
            else:
                logger.info(f"🔇 Сповіщення вимкнено. Не надсилаємо задачу {task_id} автору {creator_id}")
    except Exception as e:
        print(f"Помилка відправки сповіщення: {e}")


@router.patch("/event_in_progress", dependencies=[Depends(check_not_guest)])
async def event_in_progress(event_id, user=Depends(get_current_telegram_user)):
    await Events.update(
        {
            Events.event_status: 1,
            Events.event_who_changed_id: user.telegram_id,
            Events.event_who_changed_name: user.full_name_for_orders,
        }
    ).where(Events.event_id == event_id).run()
    changed_color_calendar_events_by_id(event_id, 1)
    telegram_data = await Events.select().where(Events.event_id == event_id)
    
    # Спробуємо підтягнути фактичні дані по доставці для події календаря
    final_data_text = ""
    try:
        delivery = await Deliveries.objects().where(Deliveries.calendar_id == event_id).first().run()
        if delivery:
            final_data_text = "\n\n" + await format_delivery_final_data(delivery.id)
    except Exception as e:
        logger.error(f"Error fetching delivery details for event in progress: {e}")

    if SEND_NOTIFICATIONS:
        await bot.send_message(
            chat_id=telegram_data[0]["event_creator"],
            text=f"Вашу доставку для {telegram_data[0]['event']} взято в роботу. Виконавець {telegram_data[0]['event_who_changed_name']}{final_data_text}",
            parse_mode="HTML",
        )
    else:
        logger.info(f"🔇 Сповіщення вимкнено. Подія {event_id} взята в роботу.")


@router.patch("/event_completed", dependencies=[Depends(check_not_guest)])
async def event_completed(event_id, user=Depends(get_current_telegram_user)):
    await Events.update(
        {
            Events.event_status: 2,
            Events.event_who_changed_id: user.telegram_id,
            Events.event_who_changed_name: user.full_name_for_orders,
        },
        force=True,
    ).where(Events.event_id == event_id).run()
    changed_color_calendar_events_by_id(event_id, 2)
    telegram_data = await Events.select().where(Events.event_id == event_id)
    if SEND_NOTIFICATIONS:
        await bot.send_message(
            chat_id=telegram_data[0]["event_creator"],
            text=f"Ваша доставка для {telegram_data[0]['event']} передана для підготовки документів, та для комплектації. Виконавець {telegram_data[0]['event_who_changed_name']}",
        )
    else:
        logger.info(f"🔇 Сповіщення вимкнено. Подія {event_id} завершена.")


@router.patch("/event_changed_date", dependencies=[Depends(check_not_guest)])
async def event_changed_date(
    event_id: str, new_date: ChangeDateRequest, user=Depends(get_current_telegram_user)
):
    await Events.update(
        {
            Events.start_event: new_date.new_date,
            Events.event_who_changed_id: user.telegram_id,
            Events.event_who_changed_name: user.full_name_for_orders,
        }
    ).where(Events.event_id == event_id).run()
    changed_date_calendar_events_by_id(event_id=event_id, new_date=new_date.new_date)
    telegram_data = await Events.select().where(Events.event_id == event_id)
    if SEND_NOTIFICATIONS:
        await bot.send_message(
            chat_id=telegram_data[0]["event_creator"],
            text=f"Для доставки {telegram_data[0]['event']} змінена дата доставки, на {new_date.new_date.day}.{new_date.new_date.month}.{new_date.new_date.year}. Виконавець {telegram_data[0]['event_who_changed_name']}",
        )
    else:
        logger.info(f"🔇 Сповіщення вимкнено. У події {event_id} змінена дата.")


@router.get("/get_task_status")
async def get_task_status(task_id):
    try:
        data = await Tasks.objects().where(Tasks.task_id == task_id).run()
        return data[0]
    except Exception as e:
        logger.error(e)
