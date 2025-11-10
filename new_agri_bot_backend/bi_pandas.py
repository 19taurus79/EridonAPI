import pandas as pd
from fastapi import APIRouter, Query
from piccolo.query import Sum
from typing import List, Optional

from new_agri_bot_backend.tables import (
    Remains,
    Submissions,
    FreeStock,
    Payment,
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

    # Запрос 1: Общий спрос на каждый товар.
    # Мы суммируем количество ('different') по каждому товару ('product') и направлению ('line_of_business')
    demand_data = (
        await Submissions.select(
            Submissions.product.product.as_alias("product"),
            Submissions.line_of_business,
            Sum(Submissions.different).as_alias("qty_needed"),
        )
        .where(
            submissions_filters
        )
        .group_by(Submissions.product.product, Submissions.line_of_business)
        .run()
    )

    # Запрос 2: Детальная информация по каждому заказу.
    # Это нужно, чтобы потом показать, из каких именно заказов состоит спрос.
    orders_data = (
        await Submissions.select(
            Submissions.manager,
            Submissions.client,
            Submissions.contract_supplement,
            Submissions.period,
            Submissions.document_status,
            Submissions.product.product.as_alias("product"),
            Submissions.different.as_alias("qty"),
        )
        .where(
            submissions_filters
        )
        .run()
    )

    # --- КЛЮЧЕВОЕ ИСПРАВЛЕНИЕ: Получаем список отфильтрованных товаров ---
    # После того как мы получили отфильтрованный спрос, мы извлекаем из него
    # уникальные названия товаров. Этот список мы будем использовать для
    # фильтрации всех остальных запросов, чтобы обеспечить консистентность данных.
    if not demand_data:
        # Если после фильтрации спроса не осталось, то и анализировать нечего.
        return {"missing_but_available": [], "missing_and_unavailable": []}

    filtered_product_names = list(set(item["product"] for item in demand_data))
    # --------------------------------------------------------------------

    # Запрос 3: Общие остатки по каждому товару.
    # Суммируем бухгалтерские остатки ('buh') для каждого товара.
    remains_data = (
        await Remains.select(
            Remains.product.product.as_alias("product"),
            Sum(Remains.buh).as_alias("qty_remain"),
        )
        .where(Remains.buh > 0, Remains.product.product.is_in(filtered_product_names))
        .group_by(Remains.product.product)
        .run()
    )

    # Запрос 4: Детальные свободные остатки на складах.
    # Получаем информацию о том, на каком складе и в каком подразделении есть свободные товары.
    available_data = (
        await FreeStock.select(
            FreeStock.product.product.as_alias("product"),
            FreeStock.division,
            FreeStock.warehouse,
            FreeStock.free_qty.as_alias("available"),
        )
        .where(FreeStock.free_qty > 0, FreeStock.product.product.is_in(filtered_product_names))
        .run()
    )
    delivery_status = await Payment.select(
        Payment.contract_supplement, Payment.order_status
    )
    # --- 2. ЗАГРУЗКА И ТРАНСФОРМАЦИЯ (Load & Transform) с Pandas ---

    # Если спроса нет, то и анализировать нечего. Сразу возвращаем пустой результат.
    if not demand_data:
        return {"missing_but_available": [], "missing_and_unavailable": []}

    # Преобразуем "сырые" данные (списки словарей) в DataFrame'ы.
    # DataFrame - это как "умная" таблица Excel, с которой очень удобно работать в коде.
    df_demand = pd.DataFrame(demand_data)
    df_remains = pd.DataFrame(remains_data)
    df_delivery_status = pd.DataFrame(delivery_status)
    # "Соединяем" таблицы спроса и остатков.
    # pd.merge - это аналог SQL-операции JOIN.
    # Мы берем таблицу спроса (df_demand) и "присоединяем" к ней справа данные из таблицы остатков (df_remains).
    # Соединение происходит по общей колонке 'product'.
    # how='left' означает, что все строки из левой таблицы (спроса) останутся, даже если для них не нашлось остатков.
    df_analysis = pd.merge(df_demand, df_remains, on="product", how="left")

    # После 'left' merge, если для какого-то товара не нашлось остатков,
    # в колонке 'qty_remain' будет стоять специальное значение NaN (Not a Number).
    # Мы заменяем все NaN на 0, чтобы можно было проводить математические операции.
    df_analysis["qty_remain"] = df_analysis["qty_remain"].fillna(0)

    # Вычисляем нехватку товара.
    # Это "векторизованная" операция: Pandas выполняет вычитание сразу для всей колонки,
    # что намного быстрее, чем делать это в цикле for.
    df_analysis["qty_missing"] = df_analysis["qty_needed"] - df_analysis["qty_remain"]

    # Фильтруем таблицу, оставляя только те строки, где нехватка (qty_missing) больше нуля.
    # .copy() используется, чтобы избежать предупреждения SettingWithCopyWarning от Pandas.
    df_analysis = df_analysis[df_analysis["qty_missing"] > 0].copy()

    # --- 3. АГРЕГАЦИЯ ДЕТАЛЬНЫХ ДАННЫХ ---
    # Теперь нам нужно добавить в нашу аналитическую таблицу детальную информацию
    # о заказах и свободных остатках, сгруппировав ее по товарам.

    # Функция для группировки строк DataFrame в список словарей.
    def aggregate_rows_to_list(group):
        # Для каждой группы (т.е. для каждого товара) преобразуем все его строки в список словарей.
        return group.to_dict("records")

    # Обрабатываем детальные заказы
    if orders_data:
        df_orders = pd.DataFrame(orders_data)
        df_orders = pd.merge(
            df_orders, df_delivery_status, on="contract_supplement", how="left"
        )

        # --- Применение фильтра по статусу заказа ---
        if order_status:
            df_orders = df_orders[df_orders["order_status"].isin(order_status)]

        df_orders["order_status"] = df_orders["order_status"].fillna("Ні")
        # Группируем все заказы по колонке 'product'.
        # Затем для каждой группы применяем нашу функцию aggregate_rows_to_list.
        # В итоге получаем Series, где индекс - это 'product', а значение - список заказов по этому продукту.
        # .rename("orders") дает имя этой новой колонке.
        orders_grouped = (
            df_orders.groupby("product").apply(aggregate_rows_to_list).rename("orders")
        )
        # Присоединяем сгруппированные заказы к нашей основной аналитической таблице.
        df_analysis = pd.merge(df_analysis, orders_grouped, on="product", how="left")
    else:
        # Если данных по заказам нет, просто создаем пустую колонку.
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

    # После 'left' merge в колонках 'orders' и 'available_stock' могут появиться NaN
    # (если для товара не нашлось заказов или остатков).
    # Заменяем эти NaN на пустые списки для консистентности данных.
    def fill_na_with_empty_list(series_element):
        # Проверяем, является ли элемент уже списком. Если нет (например, это NaN), возвращаем пустой список.
        if isinstance(series_element, list):
            return series_element
        else:
            return []

    df_analysis["orders"] = df_analysis["orders"].apply(fill_na_with_empty_list)
    df_analysis["available_stock"] = df_analysis["available_stock"].apply(
        fill_na_with_empty_list
    )

    # --- 4. ФИНАЛЬНАЯ ОБРАБОТКА И СОРТИРОВКА ---

    # Создаем функцию для сортировки списка остатков по приоритетным подразделениям.
    def sort_stock(stock_list):
        # Функция-ключ для сортировки:
        # Если подразделение есть в списке приоритетных, используем его индекс (чем меньше, тем важнее).
        # Если нет, даем ему большой индекс, чтобы оно оказалось в конце.
        def sort_key(stock_item):
            division = stock_item["division"]
            if division in priority_divisions:
                return priority_divisions.index(division)
            else:
                return len(priority_divisions)

        return sorted(
            stock_list,
            key=sort_key,
        )

    # Применяем нашу функцию сортировки к каждой ячейке в колонке 'available_stock'.
    df_analysis["available_stock"] = df_analysis["available_stock"].apply(sort_stock)

    # Добавляем вспомогательную колонку-флаг 'is_available'.
    # Она будет True, если список доступных остатков не пустой, и False в противном случае.
    # Это упростит финальную фильтрацию.
    number_of_available_stocks = df_analysis["available_stock"].apply(len)
    df_analysis["is_available"] = number_of_available_stocks > 0

    # Сортируем всю таблицу по названию продукта для упорядоченного вывода.
    # reset_index(drop=True) сбрасывает старые индексы после сортировки.
    df_analysis = df_analysis.sort_values(by="product").reset_index(drop=True)

    # --- 5. ФОРМИРОВАНИЕ ОТВЕТА ---

    # Фильтруем DataFrame по нашему флагу 'is_available'.
    df_available = df_analysis[df_analysis["is_available"]]
    df_unavailable = df_analysis[~df_analysis["is_available"]]

    # Преобразуем отфильтрованные DataFrame обратно в списки словарей для JSON-ответа.
    missing_but_available = df_available.to_dict("records")
    missing_and_unavailable = df_unavailable.to_dict("records")

    return {
        "missing_but_available": missing_but_available,
        "missing_and_unavailable": missing_and_unavailable,
    }
