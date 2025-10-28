from collections import defaultdict

from fastapi import APIRouter
from piccolo.query import Sum

from new_agri_bot_backend.tables import (
    Remains,
    Submissions,
    ProductGuide,
    AvailableStock,
    FreeStock,
)

router = APIRouter(
    prefix="/api",
    tags=["api"],
)


@router.get("/remains")
async def get_remains():
    remains_with_series = (
        await Remains.select(
            Remains.product.product.as_alias("product"),
            Remains.nomenclature_series,
            Remains.buh,
            Remains.skl,
        )
        .where(Remains.buh > 0)
        .order_by(Remains.product.product)
        .run()
    )

    remains_total = (
        await Remains.select(
            Remains.product.product.as_alias("product"),
            Sum(Remains.buh).as_alias("buh"),
            Sum(Remains.skl).as_alias("skl"),
        )
        .where(Remains.buh > 0)
        .order_by(Remains.product.product)
        .group_by(Remains.product.product)
        .run()
    )

    return {"remains_total": remains_total, "remains_with_series": remains_with_series}


# Список приоритетных подразделений. Вы можете его изменить по вашим требованиям.
priority_divisions = [
    "Центральний офіс",
    "Київський підрозділ",
    "Полтавський підрозділ",
    "Лубенський підрозділ",
    "Дніпровський підрозділ",
    "Запорізький підрозділ",
]

# Используем defaultdict для удобного добавления товаров в списки по ключам без дополнительных проверок.
from collections import defaultdict


@router.get("/combined")
async def combined_endpoint():
    # Инициализация вспомогательных словарей внутри функции, чтобы не сохранять состояние между вызовами.
    remains_map = {}
    available_map = defaultdict(list)

    # 1. Запросы к базе данных

    # Получаем спрос по продуктам — сумма всех положительных значений различий для тех документов, где статус содержит "затвердже"
    demand = (
        await Submissions.select(
            Submissions.product.product.as_alias("product"),
            Sum(Submissions.different).as_alias("qty"),
        )
        .where(
            (Submissions.different > 0)
            & (Submissions.document_status.ilike("%затвердже%"))
        )
        .group_by(Submissions.product.product)
        .run()
    )

    # Получаем остатки в бухучете продуктов с положительным количеством
    remains = (
        await Remains.select(
            Remains.product.product.as_alias("product"),
            Sum(Remains.buh).as_alias("qty"),
        )
        .where(Remains.buh > 0)
        .group_by(Remains.product.product)
        .run()
    )

    # Получаем количество свободных товаров на складах по подразделениям
    available = await FreeStock.select(
        FreeStock.product.product.as_alias("product"),
        FreeStock.division,
        FreeStock.warehouse,
        FreeStock.free_qty,
    ).run()

    # 2. Подготовка словарей для удобной работы и быстрого поиска по продуктам
    # remains_map: ключ — товар, значение — остаток в бухучете
    remains_map = {r["product"]: r["qty"] for r in remains}

    # available_map: ключ — товар, значение — список записей о доступных остатках в разрезе подразделений и складов
    for a in available:
        available_map[a["product"]].append(
            {
                "division": a["division"],
                "warehouse": a["warehouse"],
                "available": a["free_qty"],
            }
        )

    # 3. Производим анализ спроса vs остатков и формируем две группы:
    #    - missing_but_available: есть спрос превышающий остатки, но имеются доступные запасы на складах
    #    - missing_and_unavailable: спрос превышает остатки, но свободных запасов на складах нет
    missing_but_available = []
    missing_and_unavailable = []

    for d in demand:
        product = d["product"]
        qty_needed = d["qty"]
        qty_remain = remains_map.get(product, 0)  # остаток в бухучете
        qty_missing = (
            qty_needed - qty_remain
        )  # сколько не хватает для полного покрытия спроса

        if qty_missing > 0:
            # Сортируем доступные остатки по приоритету подразделений, чтобы важные подразделения шли первыми
            sorted_available_stock = sorted(
                available_map.get(product, []),
                key=lambda x: (
                    (
                        priority_divisions.index(x["division"])
                        if x["division"] in priority_divisions
                        else len(priority_divisions)
                    ),
                    x["division"],
                ),
            )

            # Формируем итоговую запись по продукту с оставшимися параметрами
            combined_item = {
                "product": product,
                "qty_needed": qty_needed,
                "qty_remain": qty_remain,
                "qty_missing": qty_missing,
                "available_stock": sorted_available_stock,
            }

            # Распределяем в соответствующий список
            if sorted_available_stock:
                missing_but_available.append(combined_item)
            else:
                missing_and_unavailable.append(combined_item)

    # 4. Сортируем итоговые списки по названию товара для удобства отображения и анализа
    missing_but_available.sort(key=lambda x: x["product"])
    missing_and_unavailable.sort(key=lambda x: x["product"])

    # Возвращаем результат в формате JSON:
    #  - товары с недостающим количеством, но с наличием на складах
    #  - товары с недостающим количеством и без наличия на складах
    return {
        "missing_but_available": missing_but_available,
        "missing_and_unavailable": missing_and_unavailable,
    }
