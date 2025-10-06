from collections import defaultdict

from fastapi import APIRouter
from piccolo.query import Sum

from new_agri_bot_backend.tables import (
    Remains,
    Submissions,
    ProductGuide,
    AvailableStock,
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


# Список приоритетных подразделений. Можете изменить его.
priority_divisions = [
    "Центральний офіс",
    "Київський підрозділ",
    "Полтавський підрозділ",
    "Лубенський підрозділ",
    "Дніпровський підрозділ",
    "Запорізький підрозділ",
]

# Используем defaultdict для более простого создания словаря available_map.
# Это небольшое улучшение, чтобы избежать if/else при добавлении.


@router.get("/combined")
async def combined_endpoint():
    # Инициализация словарей внутри функции, чтобы избежать накопления данных
    remains_map = {}
    available_map = defaultdict(list)

    # 1. Запросы к базе данных
    demand = (
        await Submissions.select(
            Submissions.product.product.as_alias("product"),
            Sum(Submissions.different).as_alias("qty"),
        )
        .where(
            (Submissions.different > 0)
            & (Submissions.document_status.ilike("%затверджено%"))
        )
        .group_by(Submissions.product.product)
        .run()
    )

    remains = (
        await Remains.select(
            Remains.product.product.as_alias("product"),
            Sum(Remains.buh).as_alias("qty"),
        )
        .where(Remains.buh > 0)
        .group_by(Remains.product.product)
        .run()
    )

    available = await AvailableStock.select(
        AvailableStock.product.product.as_alias("product"),
        AvailableStock.division,
        AvailableStock.available,
    ).run()

    # 2. Обработка и подготовка данных
    remains_map = {r["product"]: r["qty"] for r in remains}
    for a in available:
        available_map[a["product"]].append(
            {
                "division": a["division"],
                "available": a["available"],
            }
        )

    # 3. Разделение результатов на две категории
    missing_but_available = []
    missing_and_unavailable = []

    for d in demand:
        product = d["product"]
        qty_needed = d["qty"]
        qty_remain = remains_map.get(product, 0)
        qty_missing = qty_needed - qty_remain

        if qty_missing > 0:
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

            # Создаём запись для товара
            combined_item = {
                "product": product,
                "qty_needed": qty_needed,
                "qty_remain": qty_remain,
                "qty_missing": qty_missing,
                "available_stock": sorted_available_stock,
            }

            # Распределяем товар по категориям
            if sorted_available_stock:
                missing_but_available.append(combined_item)
            else:
                missing_and_unavailable.append(combined_item)

    # 4. Сортировка итоговых списков по названию продукта
    missing_but_available.sort(key=lambda x: x["product"])
    missing_and_unavailable.sort(key=lambda x: x["product"])

    return {
        "missing_but_available": missing_but_available,
        "missing_and_unavailable": missing_and_unavailable,
    }
