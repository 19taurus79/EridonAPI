# app/data_retrieval.py
from datetime import datetime, timezone
from typing import Optional, List
from collections import defaultdict
import requests
from fastapi import APIRouter, Query, HTTPException, status, Depends
from piccolo.query import Sum
from pydantic import BaseModel, Field

# from .main import get_calendar_events

# Імпортуйте ваші моделі Piccolo ORM
from .tables import (
    Remains,
    ProductGuide,
    Users,
    ClientManagerGuide,
    ProductOnWarehouse,
    Submissions,
    AvailableStock,
    AvStockProd,
    MovedData,
    ProductsForOrders,
    DetailsForOrders,
    Tasks,
)
from .telegram_auth import get_current_telegram_user
from .test import (
    get_all_tasks,
    create_task,
    get_task_by_id,
    complete_task,
    in_progress_task,
)

router = APIRouter(
    prefix="/data",  # Всі едпоінти в цьому роутері починатимуться з /data
    tags=["Отримання даних"],  # Тег для Swagger UI
    # dependencies=[Depends(get_current_telegram_user)],
)


@router.get("/remains", summary="Отримати всі залишки на складі")
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
    "/remains_group/{product_id}",
    summary="Отримати залишки за конкретним продуктом, згруповані по партії ",
)
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
    return remains


@router.get("/products", summary="Отримати список продуктів з можливістю пошуку")
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


@router.get("/product/{product_id}", summary="Отримати інформацію про продукт за ID")
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


@router.get(
    "/clients",
    summary="отримати клієнтів по менеджеру, якщо адмін то усіх ",
    dependencies=[Depends(get_current_telegram_user)],
)
async def get_clients(
    manager: dict = Depends(get_current_telegram_user), name_part: Optional[str] = None
):
    if manager["is_admin"]:
        query = ClientManagerGuide.select(ClientManagerGuide.client)
    else:
        query = ClientManagerGuide.select(ClientManagerGuide.client).where(
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
async def get_product_on_warehouse(
    category: Optional[str] = None, name_part: Optional[str] = None
):
    """
    Повертає записи про товари, по яким є залишки на складі, з бази даних.
    Опціональні фільтри:
    - `category`: Фільтрувати за назвою категорії (повна відповідність).
    - `name_part`: Фільтрувати за частиною найменування товару (нечутливий до регістру).
    """
    query = ProductOnWarehouse.select()

    if category:
        query = query.where(ProductOnWarehouse.line_of_business == category)

    if name_part:
        # Використовуємо .ilike() для регістронезалежного пошуку по частині рядка
        # Якщо ваша ORM/БД не підтримує .ilike(), можливо, знадобиться інший підхід
        query = query.where(ProductOnWarehouse.product.ilike(f"%{name_part}%"))

    product = await query.run()
    return product


@router.get("/orders/{client}")
async def get_orders(client):
    orders = (
        await Submissions.select()
        .where((Submissions.client == client) & (Submissions.different > 0))
        .run()
    )
    return orders


@router.get("/contracts/{client}")
async def get_contracts(client):
    contracts = (
        await Submissions.select(
            Submissions.contract_supplement,
            Submissions.line_of_business,
            Submissions.document_status,
        )
        .where((Submissions.different > 0) & (Submissions.client == client))
        .group_by(
            Submissions.contract_supplement,
            Submissions.line_of_business,
            Submissions.document_status,
        )
        .order_by(Submissions.contract_supplement)
        .run()
    )
    return contracts


@router.get("/contract_detail/{contract}")
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


@router.get("/sum_order_by_product/{product}")
async def get_sum_order_products(product):
    # data = (
    #     await Submissions.select()
    #     .where(
    #         (Submissions.product == product)
    #         & (Submissions.different > 0)
    #         & (Submissions.document_status == "затверджено")
    #     )
    #     .run()
    # )
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


@router.get("/order_by_product/{product}")
async def get_sum_order_products(product):
    data = (
        await Submissions.select()
        .where(
            (Submissions.product == product)
            & (Submissions.different > 0)
            & (Submissions.document_status == "затверджено")
        )
        .run()
    )
    # total_sum = (
    #     await Submissions.select(Sum(Submissions.different))
    #     .where(
    #         (Submissions.product == product)
    #         & (Submissions.different > 0)
    #         & (Submissions.document_status == "затверджено")
    #     )
    #     .run()
    # )
    return data


@router.get("/moved_products_for_order/{order}")
async def get_moved_products_for_order(order: str):
    data = await MovedData.select().where(MovedData.contract == order)
    return data


@router.get("/products_for_all_orders")
async def get_products_for_all_orders():
    data = await ProductsForOrders.select().run()
    return data


@router.get("/party_data")
async def get_party_data(party: str):
    data = (
        await Remains.select(
            Remains.crop_year,
            Remains.germination,
            Remains.mtn,
            Remains.origin_country,
            Remains.weight,
        )
        .where(Remains.id == party)
        .run()
    )
    return data


@router.get("/id_in_remains")
async def get_id_in_remains(party: str):
    data = (
        await Remains.select(Remains.id)
        .where(Remains.nomenclature_series == party)
        .run()
    )
    return data


def group_products_with_parties(items):
    grouped = defaultdict(
        lambda: {
            "id": None,
            "nomenclature": None,
            "party_sign": None,
            "buying_season": None,
            "different": None,
            "client": None,
            "contract_supplement": None,
            "manager": None,
            "product": None,
            "orders_q": None,
            "buh": None,
            "skl": None,
            "qok": True,
            "parties": [],
        }
    )

    for item in items:
        product_uuid = item["product"]
        group = grouped[product_uuid]

        # Инициализация данных в группе, если не инициализирована
        if group["product"] is None:
            group["id"] = str(item.get("id")) if item.get("id") else None
            group["nomenclature"] = item.get("nomenclature")
            group["party_sign"] = item.get("party_sign")
            group["buying_season"] = item.get("buying_season")
            group["different"] = (
                float(item.get("different"))
                if item.get("different") is not None
                else None
            )
            group["client"] = item.get("client")
            group["contract_supplement"] = item.get("contract_supplement")
            group["manager"] = item.get("manager")
            group["product"] = str(product_uuid)
            group["orders_q"] = (
                float(item.get("orders_q"))
                if item.get("orders_q") is not None
                else None
            )
            group["buh"] = (
                float(item.get("buh")) if item.get("buh") is not None else None
            )
            group["skl"] = (
                float(item.get("skl")) if item.get("skl") is not None else None
            )
            group["qok"] = bool(item.get("qok", True))

        else:
            # Обновление флага qok, если нужно
            group["qok"] = group["qok"] and bool(item.get("qok", True))

        # Добавляем партию
        party_data = {
            "moved_q": (
                float(item.get("moved_q")) if item.get("moved_q") is not None else 0
            ),
            "party": item.get("party"),
        }
        group["parties"].append(party_data)

    # Возвращаем именно список словарей (без product в качестве ключа)
    return list(grouped.values())


@router.get("/details_for_orders/{order}")
async def get_details_for_order(order: str):
    data = await DetailsForOrders.select().where(
        DetailsForOrders.contract_supplement == order
    )
    result = group_products_with_parties(data)
    return result


@router.get("/calendar_events")
def get_events(start: Optional[str] = None, end: Optional[str] = None):
    from .main import get_calendar_events

    data = get_calendar_events(start_date=start, end_date=end)
    return data


@router.get("/get_all_tasks")
async def get_tasks(user=Depends(get_current_telegram_user)):
    data = await get_all_tasks(user)
    return data


class TaskCreate(BaseModel):
    date: Optional[datetime] = Field(default_factory=lambda: datetime.utcnow())
    title: str
    note: str


@router.post("/add_task")
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


@router.patch("/task_in_progress")
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


@router.patch("/task_completed")
async def task_completed(task_id, user=Depends(get_current_telegram_user)):
    await Tasks.update(
        {
            Tasks.task_status: 2,
            Tasks.task_who_changed_id: user.telegram_id,
            Tasks.task_who_changed_name: user.full_name_for_orders,
        },
        force=True,
    ).where(Tasks.task_id == task_id).run()
    complete_task(task_id, user)


@router.get("/get_task_status")
async def get_task_status(task_id):
    try:
        data = await Tasks.objects().where(Tasks.task_id == task_id).run()
        return data[0]
    except Exception as e:
        print(e)
