import pandas as pd
from fastapi import APIRouter, Query
from piccolo.columns import Integer, Float
from piccolo.query import Sum
from typing import List, Optional

from piccolo.query.functions import Cast

from new_agri_bot_backend.tables import (
    Remains,
    Submissions,
    FreeStock,
    Payment,
    MovedData,
)

router = APIRouter(
    prefix="/api",  # Используем новый префикс для пандас-версии
    tags=["api-pandas"],
)

# Список приоритетных подразделений остается тем же
priority_divisions = [
    "Центральний офіс",
    "Київський підрозділ",
    "Полтавський підрозділ",
    "Лубенський підрозділ",
    "Дніпровський підрозділ",
    "Запорізький підрозділ",
]


@router.get("/combined")
async def combined_pandas_endpoint(
    document_status: Optional[List[str]] = Query(
        None, description="Список статусів документів для фільтрації"
    ),
    order_status: Optional[List[str]] = Query(
        None, description="Список статусів замовлень для фільтрації"
    ),
):
    # --- 1. ИЗВЛЕЧЕНИЕ ДАННЫХ (Extract) ---
    # На этом этапе мы выполняем асинхронные запросы к базе данных,
    # чтобы получить все необходимые "сырые" данные для анализа.
    # Каждый запрос агрегирует данные на стороне БД, чтобы уменьшить объем передаваемой информации.

    # --- Формирование динамических фильтров для Submissions ---
    submissions_filters = Submissions.different > 0
    if document_status:
        submissions_filters &= Submissions.document_status.is_in(document_status)
    else:
        # Поведение по умолчанию, если фильтр не передан
        submissions_filters &= Submissions.document_status.ilike("%затвердже%")
    if order_status:
        submissions_filters &= Submissions.delivery_status.is_in(order_status)
    else:
        # Поведение по умолчанию, если фильтр не передан
        submissions_filters &= Submissions.document_status.ilike("%затвердже%")

    # Запрос 1: Общий спрос на каждый товар.
    # Мы суммируем количество ('different') по каждому товару ('product') и направлению ('line_of_business')
    demand_data = (
        await Submissions.select(
            Submissions.product.product.as_alias("product"),
            Submissions.line_of_business,
            Sum(Submissions.different).as_alias("qty_needed"),
        )
        .where(submissions_filters)
        .group_by(Submissions.product.product, Submissions.line_of_business)
        .run()
    )

    # Если после фильтрации спроса не осталось, то и анализировать нечего.
    if not demand_data:
        return {"missing_but_available": [], "missing_and_unavailable": []}

    # Запрос 2: Детальная информация по каждому заказу.
    orders_data = (
        await Submissions.select(
            Submissions.manager,
            Submissions.client,
            Submissions.contract_supplement,
            Submissions.period,
            Submissions.document_status,
            Submissions.delivery_status,
            Submissions.product.product.as_alias("product"),
            Submissions.different.as_alias("qty"),
        )
        .where(submissions_filters)
        .run()
    )

    filtered_product_names = list(set(item["product"] for item in demand_data))

    # Запрос 3: Общие остатки по ВСЕМ товарам.
    # Фильтрация будет происходить позже на стороне Pandas.
    remains_data = (
        await Remains.select(
            Remains.product.product.as_alias("product"),
            Sum(Remains.buh).as_alias("qty_remain"),
        )
        .where(Remains.buh > 0)
        .group_by(Remains.product.product)
        .run()
    )

    # Запрос 4: Детальные свободные остатки на складах (только для нужных товаров).
    available_data = (
        await FreeStock.select(
            FreeStock.product.product.as_alias("product"),
            FreeStock.division,
            FreeStock.warehouse,
            FreeStock.free_qty.as_alias("available"),
        )
        .where(
            FreeStock.free_qty > 0,
            FreeStock.product.product.is_in(filtered_product_names),
        )
        .run()
    )
    moved_data = (
        await MovedData.select(
            MovedData.contract,
            MovedData.product,
            Sum(Cast(MovedData.qt_moved, Float())),
        )
        .group_by(MovedData.contract, MovedData.product)
        .run()
    )
    # delivery_status = await Payment.select(
    #     Payment.contract_supplement, Payment.order_status
    # )
    # --- 2. ЗАГРУЗКА И ТРАНСФОРМАЦИЯ (Load & Transform) с Pandas ---

    df_demand = pd.DataFrame(demand_data)
    df_remains = pd.DataFrame(remains_data)
    df_moved = pd.DataFrame(moved_data)
    # df_delivery_status = pd.DataFrame(delivery_status)

    # "Соединяем" таблицы спроса и остатков (LEFT JOIN).
    df_analysis = pd.merge(df_demand, df_remains, on="product", how="left")

    # Заменяем NaN (отсутствие остатков) на 0.
    df_analysis["qty_remain"] = df_analysis["qty_remain"].fillna(0)

    # Вычисляем нехватку товара.
    df_analysis["qty_missing"] = df_analysis["qty_needed"] - df_analysis["qty_remain"]

    # Оставляем только те строки, где есть нехватка.
    df_analysis = df_analysis[df_analysis["qty_missing"] > 0].copy()

    # --- 3. АГРЕГАЦИЯ ДЕТАЛЬНЫХ ДАННЫХ ---
    def aggregate_rows_to_list(group):
        return group.to_dict("records")

    # Обрабатываем детальные заказы
    if orders_data:
        df_orders = pd.DataFrame(orders_data)
        # df_orders = pd.merge(
        #     df_orders, df_delivery_status, on="contract_supplement", how="left"
        # )
        # df_orders["order_status"] = df_orders["order_status"].fillna("Ні")
        orders_grouped = (
            df_orders.groupby("product").apply(aggregate_rows_to_list).rename("orders")
        )
        df_analysis = pd.merge(df_analysis, orders_grouped, on="product", how="left")
    else:
        df_analysis["orders"] = [[] for _ in range(len(df_analysis))]

    # Делаем то же самое для свободных остатков на складах.
    if available_data:
        df_available = pd.DataFrame(available_data)
        available_grouped = (
            df_available.groupby("product")
            .apply(aggregate_rows_to_list)
            .rename("available_stock")
        )
        df_analysis = pd.merge(df_analysis, available_grouped, on="product", how="left")
    else:
        df_analysis["available_stock"] = [[] for _ in range(len(df_analysis))]

    def fill_na_with_empty_list(series_element):
        if isinstance(series_element, list):
            return series_element
        else:
            return []

    df_analysis["orders"] = df_analysis["orders"].apply(fill_na_with_empty_list)
    df_analysis["available_stock"] = df_analysis["available_stock"].apply(
        fill_na_with_empty_list
    )

    # --- 4. ФИНАЛЬНАЯ ОБРАБОТКА И СОРТИРОВКА ---
    def sort_stock(stock_list):
        def sort_key(stock_item):
            division = stock_item["division"]
            if division in priority_divisions:
                return priority_divisions.index(division)
            else:
                return len(priority_divisions)

        return sorted(stock_list, key=sort_key)

    df_analysis["available_stock"] = df_analysis["available_stock"].apply(sort_stock)

    number_of_available_stocks = df_analysis["available_stock"].apply(len)
    df_analysis["is_available"] = number_of_available_stocks > 0

    df_analysis = df_analysis.sort_values(by="product").reset_index(drop=True)

    # --- 5. ФОРМИРОВАНИЕ ОТВЕТА ---
    df_available = df_analysis[df_analysis["is_available"]]
    df_unavailable = df_analysis[~df_analysis["is_available"]]

    missing_but_available = df_available.to_dict("records")
    missing_and_unavailable = df_unavailable.to_dict("records")

    return {
        "missing_but_available": missing_but_available,
        "missing_and_unavailable": missing_and_unavailable,
    }
