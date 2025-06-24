# app/data_retrieval.py
from typing import Optional, List
from fastapi import APIRouter, Query, HTTPException, status, Depends

# Імпортуйте ваші моделі Piccolo ORM
from .tables import Remains, ProductGuide, Users, ClientManagerGuide, ProductOnWarehouse
from .telegram_auth import get_current_telegram_user

router = APIRouter(
    prefix="/data",  # Всі едпоінти в цьому роутері починатимуться з /data
    tags=["Отримання даних"],  # Тег для Swagger UI
    dependencies=[Depends(get_current_telegram_user)],
)


@router.get("/remains", summary="Отримати всі залишки на складі")
async def get_remains():
    """
    Повертає всі записи про залишки на складі з бази даних.
    """
    remains = await Remains.select().run()
    return remains


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


@router.get("/products", summary="Отримати список продуктів з можливістю пошуку")
async def get_products(
    query: Optional[str] = Query(
        None, description="Рядок для пошуку за назвою продукту"
    )
):
    """
    Повертає список всіх продуктів.
    Можна використовувати параметр `query` для пошуку продуктів за частиною назви (без урахування регістру).
    """
    if query:
        # Якщо ProductGuide.product – це поле, за яким ви хочете шукати
        products = (
            await ProductGuide.select()
            .where(
                ProductGuide.product.ilike(
                    f"%{query}%"
                )  # Нечутливий до регістру пошук LIKE
            )
            .run()
        )
    else:
        products = await ProductGuide.select().run()
    return products


@router.get("/product/{product_id}", summary="Отримати інформацію про продукт за ID")
async def get_product_by_id(
    product_id: int,
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


@router.get("/clients", summary="отримати клієнтів по менеджеру, якщо адмін то усіх ")
async def get_clients(manager: dict = Depends(get_current_telegram_user)):
    if manager["is_admin"]:
        clients = await ClientManagerGuide.select(ClientManagerGuide.client)
    else:
        clients = (
            await ClientManagerGuide.select(ClientManagerGuide.client)
            .where(ClientManagerGuide.manager == manager["full_name_for_orders"])
            .run()
        )
    return clients


@router.get(
    "/product_on_warehouse",
    summary="Отримати товари, по яким є залишки на складі, з опціональними фільтрами",
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
