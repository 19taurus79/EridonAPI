from __future__ import annotations
import csv
import json
import io
import os
import tempfile
import uuid
from enum import Enum
from pathlib import Path
from typing import Optional, List, Dict

import pandas as pd
import uvicorn
import asyncio
from concurrent.futures import ThreadPoolExecutor
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
from openpyxl.utils import get_column_letter
from asyncpg import UniqueViolationError
from piccolo.columns.defaults import TimestampNow
from . import models, processing
from .google_calendar import (
    create_calendar_event,
    get_calendar_events,
    get_calendar_event_by_id,
    changed_color_calendar_events_by_id,
    changed_date_calendar_events_by_id,
    delete_calendar_event_by_id,
)
from .models import (
    RegionResponse, 
    AddressResponse, 
    AddressCreate, 
    DeliveryRequest, 
    DeleteDeliveryRequest,
    UpdateDeliveryRequest, 
    BatchUpdateDeliveryRequest,
    ChangeDeliveryDateRequest,
    CreateCommentRequest,
    UpdateCommentRequest,
    CommentResponse,
    CommentType,
    ClientData,
    Order,
    Product
)
from .tables import (
    Remains,
    Events,
    AddressGuide,
    Submissions,
    ClientAddress,
    MovedData,
    Deliveries,
    DeliveryItems,
    OrderComments,
    ScheduledDeletions,
)
from aiogram.types import FSInputFile
from fastapi import (
    FastAPI,
    UploadFile,
    File,
    HTTPException,
    status,
    BackgroundTasks,
    Depends,
    Query,
    Form,
    Request,
    Header,
)
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timezone, timedelta
from piccolo_admin.endpoints import create_admin

from pydantic import BaseModel, Field, validator

# Импорты из ваших новых модулей
from .telegram_auth import (
    router as telegram_auth_router,
    InitDataModel,
    check_telegram_auth,
    get_current_telegram_user,
    check_not_guest,
)
from .data_retrieval import router as data_retrieval_router
from .data_loader import save_processed_data_to_db
from .bi import router as bi_router
from .bi_pandas import router as bi_pandas_router
from .order_chat import router as chat_router
from .notification import router as notification_router
from .nova_poshta import router as nova_poshta_router
from .bot_handlers import setup_bot_handlers
from .scheduler import setup_scheduler
from .utils import send_message_to_managers, create_composite_key_from_dict
from .delivery_notifications import notify_new_delivery, notify_delivery_status_change, delete_delivery_notifications

# Импорт TELEGRAM_BOT_TOKEN из config.py для инициализации бота
# Импорт констант из config.py
from .config import (
    TELEGRAM_BOT_TOKEN, 
    bot, 
    logger, 
    BACKEND_URL,
    CORS_ORIGINS,
    SEND_NOTIFICATIONS,
    LOGISTICS_TELEGRAM_IDS,
)

# Инициализация Telegram Bot (используется в utils.py, но может быть нужен здесь для глобальной инициализации)
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.types import Update
from .telegram_auth import confirm_login_token
from google.oauth2 import service_account
from googleapiclient.discovery import build
from pydantic import BaseModel
from datetime import date

# Важно: если бот не используется напрямую в main, эту строку можно убрать



# models.py contains all the Pydantic models for the API





admin_router = create_admin([Remains], allowed_hosts=["localhost"])

sessions = {}


def get_fallback_weight(line_of_business: str, nomenclature: str) -> float:
    """
    Вычисляет резервный вес на основе бизнес-логики, если вес отсутствует в Remains.
    """
    # Карта для простых случаев
    LOB_WEIGHT_MAP = {
        "Власне виробництво насіння": 1.0,
        "ЗЗР": 1.2,
        "Міндобрива (основні)": 1000.0,
    }

    if line_of_business in LOB_WEIGHT_MAP:
        return LOB_WEIGHT_MAP[line_of_business]

    # Сложный случай для "Насіння"
    if line_of_business == "Насіння":
        if "(150К)" in nomenclature:
            return 10.0
        if "(50К)" in nomenclature:
            return 15.0
        if "(80К)" in nomenclature:
            return 20.0

    # Если ни одно из правил не подошло, возвращаем 0
    return 0.0


# aiogram Dispatcher для обработки входящих сообщений бота
dp = Dispatcher()
setup_bot_handlers(dp)


# Определяем контекстный менеджер для жизненного цикла приложения
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(
        "Piccolo database engine initialized. Connections will be managed automatically."
    )
    # Перевірка наявності таблиць для сповіщень
    try:
        await ScheduledDeletions.create_table(if_not_exists=True).run()
    except Exception as e:
        logger.error(f"Failed to ensure ScheduledDeletions table: {e}")

    # Ініціалізація планувальника повідомлень
    setup_scheduler()
    # Регистрируем webhook для бота если есть BACKEND_URL
    if BACKEND_URL:
        webhook_url = f"{BACKEND_URL}/webhook/bot"
        try:
            await bot.set_webhook(webhook_url)
            logger.info(f"Telegram webhook registered: {webhook_url}")
        except Exception as e:
            logger.info(f"Failed to set webhook: {e}")
    yield
    # Видаляем webhook при остановке
    if BACKEND_URL:
        try:
            await bot.delete_webhook()
            logger.info("Telegram webhook removed.")
        except Exception:
            pass
    logger.info("Piccolo database engine shutdown. Connections are closed automatically.")



app = FastAPI(
    title="Data Loader API for Agri-Bot",
    description="API for loading and processing various Excel data into PostgreSQL.",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health", tags=["System"])
async def health_check():
    """Перевірка працездатності сервісу."""
    return {
        "status": "ok",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "version": "1.0.0"
    }

# --- Подключение маршрутов ---
app.include_router(telegram_auth_router)  # Подключаем маршруты из telegram_auth.py
app.include_router(data_retrieval_router)
app.include_router(bi_router)
app.include_router(bi_pandas_router)
app.include_router(chat_router)
app.include_router(notification_router)
app.include_router(nova_poshta_router)
app.mount("/admin", admin_router)


def json_to_csv_temp(data: List[ClientData]) -> str:
    """
    Зберігає дані у тимчасовий CSV файл.
    """
    fd, path = tempfile.mkstemp(suffix=".csv", prefix="orders_")
    try:
        with os.fdopen(fd, mode="w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "Клиент",
                    "Менеджер",
                    "Заказ",
                    "Продукт",
                    "Количество",
                    "Адрес доставки",
                    "Контактное лицо",
                    "Дата доставки",
                ]
            )

            for client in data:
                for order in client.orders:
                    for product in order.products:
                        writer.writerow(
                            [
                                client.client,
                                client.manager,
                                order.order,
                                product.product,
                                product.quantity,
                                client.deliveryAddress or "",
                                client.contactPerson or "",
                                client.deliveryDate or "",
                            ]
                        )
        return path
    except Exception as e:
        os.close(fd)
        logger.error(f"Помилка створення тимчасового CSV: {e}")
        raise


@app.post("/webhook/bot", include_in_schema=False)
async def bot_webhook(request: Request):
    """Отримує оновлення від Telegram та передає до aiogram Dispatcher."""
    data = await request.json()
    try:
        update = Update.model_validate(data)
    except Exception:
        update = Update(**data)
    await dp.feed_update(bot=bot, update=update)
    return {"ok": True}



#



def format_message(data: List[ClientData]) -> str:
    lines = []
    for client in data:
        lines.append(f"🧑‍💼 <b>Клиент:</b> {client.client}")
        lines.append(f"👨‍💼 <b>Менеджер:</b> {client.manager}")
        lines.append("📦 <b>Заказы:</b>")
        for order in client.orders:
            lines.append(f"  🆔 <b>Заказ:</b> <code>{order.order}</code>")
            for product in order.products:
                lines.append(
                    f"    • <code>{product.product}</code> — <b>{product.quantity}</b> "
                )
        if client.deliveryAddress:
            lines.append(f"🏠 <b>Адрес доставки:</b> {client.deliveryAddress}")
        if client.contactPerson:
            lines.append(f"📞 <b>Контактное лицо:</b> {client.contactPerson}")
        if client.deliveryDate:
            lines.append(f"📅 <b>Дата доставки:</b> {client.deliveryDate}")

    return "\n".join(lines)




@app.post("/send_telegram_message/")
async def send_telegram_message(
    data: List[ClientData],
    chat_id: int = Query(..., description="Telegram chat id для отправки сообщения"),
):
    message_text = format_message(data)
    csv_path = None
    try:
        csv_path = json_to_csv_temp(data)
        await bot.send_document(
            chat_id=chat_id, document=FSInputFile(csv_path, filename="orders.csv")
        )
        return {"status": "ok", "message": "Повідомлення та CSV файл відправлені"}
    except Exception as e:
        logger.error(f"Error in send_telegram_message: {e}")
        return {"status": "error", "details": str(e)}
    finally:
        if csv_path and os.path.exists(csv_path):
            try:
                os.remove(csv_path)
            except Exception:
                pass


class TelegramMessage(BaseModel):  # ← ДОБАВЬ ЭТО
    chat_id: int  # ← ТВОИ поля из RN
    text: str


@app.post("/send_telegram_message_by_event")
async def message(message: TelegramMessage):
    if SEND_NOTIFICATIONS:
        await bot.send_message(
            text=message.text, chat_id=message.chat_id, parse_mode="HTML"
        )
    else:
        logger.info(f"🔇 Сповіщення вимкнено. Ендпоінт /send_telegram_message_by_event пропущено.")


# --- Маршрут для загрузки и обработки данных ---
@app.post(
    "/upload_ordered_moved", response_model=models.UploadResponse, tags=["Processing"]
)
async def upload_and_process_files(
    ordered_file: UploadFile = File(..., description="Файл 'Заказано.xlsx'"),
    moved_file: UploadFile = File(..., description="Файл 'Перемещено.xlsx'"),
):
    try:
        leftovers, matched_list = processing.process_uploaded_files(
            ordered_file.file, moved_file.file
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Ошибка при обработке файлов: {e}")

    # --- НОВОВВЕДЕНИЕ: Фильтрация уже сопоставленных данных ---
    try:
        # 1. Получаем все ранее сопоставленные записи из БД
        existing_moved_records = await MovedData.select(
            MovedData.order, MovedData.product, MovedData.party_sign, MovedData.qt_moved
        ).run()

        # 2. Создаем множество уникальных ключей для быстрой проверки
        existing_keys: Set[str] = {
            create_composite_key_from_dict(
                rec, ["order", "product", "party_sign", "qt_moved"]
            )
            for rec in existing_moved_records
        }

        # 3. Фильтруем 'leftovers', удаляя уже существующие записи
        filtered_leftovers = {}
        for leftover_id, leftover_data in leftovers.items():
            moved_item = leftover_data["current_moved"][
                0
            ]  # В каждой задаче только одно перемещение
            # Названия колонок в 'moved_item' из Excel
            item_key = create_composite_key_from_dict(
                moved_item,
                [
                    "Заявка на відвантаження",
                    "Товар",
                    "Партія номенклатури",
                    "Перемещено",
                ],
            )
            if item_key not in existing_keys:
                filtered_leftovers[leftover_id] = leftover_data
        leftovers = (
            filtered_leftovers  # Заменяем оригинальные leftovers отфильтрованными
        )
    except Exception as e:
        logger.info(
            f"!!! Предупреждение: не удалось отфильтровать исторические данные. Ошибка: {e}"
        )
    # ---------------------------------------------------------

    session_id = str(uuid.uuid4())

    for req_id, data in leftovers.items():
        data["current_moved"] = pd.DataFrame(data["current_moved"]).set_index("index")
        data["current_notes"] = pd.DataFrame(data["current_notes"]).set_index("index")

    sessions[session_id] = {"leftovers": leftovers, "matched_list": matched_list}

    response_leftovers = processing.convert_numpy_types(leftovers)
    for req_id, data in response_leftovers.items():
        data["current_moved"] = data["current_moved"].reset_index().to_dict("records")
        data["current_notes"] = data["current_notes"].reset_index().to_dict("records")

    return {"session_id": session_id, "leftovers": response_leftovers}


@app.post(
    "/process/{session_id}/manual_match",
    response_model=models.MatchResponse,
    tags=["Processing"],
)
async def manual_match(session_id: str, match_input: models.ManualMatchInput):
    """
    Эндпоинт для ручного сопоставления с УЛУЧШЕННЫМ АЛГОРИТМОМ.
    Теперь поддерживает частичное сопоставление (когда суммы не равны).
    """
    if session_id not in sessions:
        raise HTTPException(status_code=404, detail="Сессия не найдена.")

    session_data = sessions[session_id]
    request_id = match_input.request_id

    if request_id not in session_data["leftovers"]:
        raise HTTPException(
            status_code=404, detail=f"Заявка с ID {request_id} не найдена."
        )

    leftover_data = session_data["leftovers"][request_id]
    current_moved_df = leftover_data["current_moved"]
    current_notes_df = leftover_data["current_notes"]

    # Извлекаем индексы из нового формата запроса
    selected_moved_indices = [item.index for item in match_input.selected_moved_items]

    try:
        # Проверяем наличие всех нужных строк перед началом обработки
        # current_moved_df.loc[selected_moved_indices]
        selected_moved = current_moved_df.loc[selected_moved_indices]
        selected_notes = current_notes_df.loc[match_input.selected_notes_indices]
    except KeyError:
        raise HTTPException(
            status_code=400,
            detail="Ошибка: одна или несколько выбранных позиций уже были сопоставлены ранее.",
        )

    newly_matched = []
    product = leftover_data["product"]
    # --- НОВЫЙ УПРОЩЕННЫЙ АЛГОРИТМ ---
    # Мы доверяем ручному выбору пользователя и не проводим строгих проверок по сумме.
    # Просто создаем сопоставленные записи на основе выбора.

    if selected_moved.empty or selected_notes.empty:
        raise HTTPException(
            status_code=400,
            detail="Необходимо выбрать хотя бы одну позицию из 'перемещено' и одну из 'примечаний'.",
        )

    # Используем информацию из первого выбранного примечания (договор)
    # для всех сопоставляемых перемещений.
    main_note_row = selected_notes.iloc[0]
    main_contract = main_note_row["Договор"]

    # Проходим по каждому элементу, который выбрал пользователь
    for selected_item in match_input.selected_moved_items:
        moved_index = selected_item.index
        requested_qty = selected_item.quantity

        # Получаем строку из DataFrame по индексу
        moved_row = current_moved_df.loc[moved_index]
        available_qty = moved_row["Перемещено"]

        # Проверка, что запрошенное количество не превышает доступное
        if requested_qty > available_qty:
            raise HTTPException(
                status_code=400,
                detail=f"Ошибка: Попытка списать {requested_qty} по позиции с индексом {moved_index}, но доступно только {available_qty}.",
            )

        # Создаем новую сопоставленную запись
        record = moved_row.to_dict()
        record["Договор"] = main_contract
        # Количество берем из запроса, а не всю доступную сумму
        record["Количество"] = requested_qty
        record["Источник"] = "Ручное сопоставление"
        newly_matched.append(record)

        # --- Логика списания ---
        remaining_qty = available_qty - requested_qty
        if remaining_qty > 0:
            # Частичное списание: обновляем остаток
            current_moved_df.loc[moved_index, "Перемещено"] = remaining_qty
        else:
            # Полное списание: удаляем строку
            current_moved_df.drop(moved_index, inplace=True)

    # Обновляем состояние "примечаний" (удаляем выбранные)
    try:
        current_notes_df.drop(match_input.selected_notes_indices, inplace=True)
    except KeyError:
        # Эта ошибка может возникнуть, если фронтенд отправит уже удаленные индексы.
        # Мы можем ее проигнорировать или вернуть предупреждение.
        logger.info(
            f"Предупреждение: Попытка удалить уже сопоставленные индексы для сессии {session_id}"
        )
        pass

    # --- КОНЕЦ НОВОГО АЛГОРИТМА ---

    session_data["matched_list"].extend(newly_matched)

    if leftover_data["current_moved"].empty or leftover_data["current_notes"].empty:
        del session_data["leftovers"][request_id]

    return {
        "message": "Ручное сопоставление успешно обработано",
        "session_id": session_id,
        "session_data": session_data,
    }


@app.get(
    "/process/{session_id}/results",
    response_model=models.ResultsResponse,
    tags=["Processing"],
)
async def get_results(session_id: str):
    if session_id not in sessions:
        raise HTTPException(status_code=404, detail="Сессия не найдена.")

    session_data = sessions[session_id]

    unmatched_by_request = {}
    response_leftovers = processing.convert_numpy_types(session_data["leftovers"])
    for req_id, data in response_leftovers.items():
        unmatched_by_request[req_id] = {
            "unmatched_moved": data["current_moved"].reset_index().to_dict("records"),
            "unmatched_notes": data["current_notes"].reset_index().to_dict("records"),
        }

    return {
        "matched_data": session_data["matched_list"],
        "unmatched_by_request": unmatched_by_request,
    }


@app.post(
    "/upload-data",
    summary="Загрузить и обработать Excel-файлы",
    response_description="Статус загрузки данных и уведомление",
)
async def upload_data(
    background_tasks: BackgroundTasks,
    av_stock_file: UploadFile = File(
        ..., description="Файл с доступными остатками по подразделению"
    ),
    remains_file: UploadFile = File(..., description="Файл с остатками"),
    submissions_file: UploadFile = File(..., description="Файл с заявками"),
    payment_file: UploadFile = File(..., description="Файл с оплатой"),
    # moved_file: UploadFile = File(..., description="Файл с перемещенными данными"),
    free_stock: UploadFile = File(
        default=..., description="Файл с доступными остатками"
    ),
    manual_matches_json: Optional[str] = Form(
        None, description="JSON-строка с результатами ручного сопоставления"
    ),
):
    """
    Принимает несколько Excel-файлов, обрабатывает их и загружает данные в базу данных.
    Обработка данных выполняется в фоновом режиме.
    После успешной загрузки отправляется уведомление менеджерам в Telegram.
    """
    logger.info(f"[{datetime.now(timezone.utc)}] Получен запрос на загрузку данных.")

    try:
        # Читаем содержимое файлов в байты асинхронно
        av_stock_content = await av_stock_file.read()
        remains_content = await remains_file.read()
        submissions_content = await submissions_file.read()
        payment_content = await payment_file.read()
        # moved_content = await moved_file.read()
        free_stock_content = await free_stock.read()

        # Запускаем синхронную функцию обработки и сохранения в базу данных
        # в отдельном потоке, чтобы не блокировать ASGI-сервер.
        background_tasks.add_task(
            save_processed_data_to_db,  # Передаем функцию
            av_stock_content,
            remains_content,
            submissions_content,
            payment_content,
            # moved_content,
            free_stock_content,
            manual_matches_json,
        )
        background_tasks.add_task(
            send_message_to_managers
        )  # Добавляем задачу по отправке уведомлений

        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED,
            content={
                "message": "Загрузка и обработка данных начаты в фоновом режиме. Уведомление будет отправлено после завершения."
            },
        )

    except Exception as e:
        logger.error(f"Ошибка при обработке загруженных файлов: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Ошибка обработки файлов: {e}",
        )


# 1. Получение списка областей (и городов со спец. статусом)
@app.get("/regions", response_model=List[RegionResponse])
async def get_regions():
    # Выбираем категории 'O' (Области) и 'K' (Киев, Севастополь)
    # Сортируем по названию
    regions = (
        await AddressGuide.select(AddressGuide.level_1_id, AddressGuide.name)
        .distinct()
        .where(AddressGuide.category.is_in(["O"]))
        .order_by(AddressGuide.name)
        .run()
    )

    return regions


# 2. Поиск населенного пункта в области
@app.get("/get_all_orders_and_address")
async def get_all_orders_and_address():
    """
    Возвращает список заказов с вычисленным общим весом и список адресов.
    Применяет резервную логику расчета веса, если он отсутствует в остатках.
    """
    # Шаг 1: Агрегируем средний вес из Remains
    weight_map = {}
    try:
        # Используем REPLACE для замены запятой на точку, чтобы корректно преобразовать в число
        avg_weight_query = """
            SELECT
                product,
                AVG(CAST(REPLACE(NULLIF(weight, ''), ',', '.') AS NUMERIC)) as avg_weight
            FROM
                remains
            WHERE
                weight IS NOT NULL AND weight != '' AND product IS NOT NULL
            GROUP BY
                product
        """
        avg_weights_list = await Remains.raw(avg_weight_query)
        weight_map = {
            item["product"]: float(item["avg_weight"] or 0) for item in avg_weights_list
        }
    except Exception as e:
        logger.info(f"--- Ошибка при запросе среднего веса: {e} ---")

    # Шаг 2: Получаем все заказы
    orders_list = await Submissions.select().where(Submissions.different > 0).run()

    # Шаг 3: Обогащаем заказы данными о весе с резервной логикой
    for order in orders_list:
        product_id = order.get("product")
        # Пытаемся получить вес из остатков
        weight_from_remains = weight_map.get(product_id)

        final_weight = 0.0
        if weight_from_remains and weight_from_remains > 0:
            # Если вес в остатках есть и он больше нуля, используем его
            final_weight = weight_from_remains
        else:
            # Иначе — применяем резервную логику
            # Используем 'or ""' чтобы гарантировать строку, даже если в базе None
            line_of_business = order.get("line_of_business") or ""
            nomenclature = order.get("nomenclature") or ""
            final_weight = get_fallback_weight(line_of_business, nomenclature)

        quantity = order.get("different", 0)
        order["total_weight"] = quantity * final_weight

    # Запрос адресов остается без изменений
    address = await ClientAddress.select().run()

    return orders_list, address


@app.get("/get_all_addresses")
async def get_all_addresses():
    address = await ClientAddress.select().run()
    return address


@app.get("/get_address_by_client/{client}")
async def get_address_by_client(client):
    address = await ClientAddress.select().where(ClientAddress.client == client).run()
    return address


@app.put("/update_address_for_client/{id}", dependencies=[Depends(check_not_guest)])
async def update_address_for_client(address_data: AddressCreate, id: int):
    obj = await ClientAddress.objects().get(where=(ClientAddress.id == id))
    data_dict = address_data.dict()
    full_address_str = data_dict.pop("address", None)
    if not full_address_str:
        raise HTTPException(
            status_code=400, detail="Поле 'full_address' обязательно для заполнения."
        )

    # 2. Разбираем строку адреса на части
    address_parts = [part.strip() for part in full_address_str.split(",")]

    data_dict["region"] = address_parts[0].split()[0]
    data_dict["area"] = address_parts[1].split()[0]
    data_dict["commune"] = address_parts[2].split()[0]
    data_dict["city"] = address_parts[3]
    obj.client = data_dict.get("client", obj.client)
    obj.manager = data_dict.get("manager", obj.manager)
    obj.representative = data_dict.get("representative", obj.representative)
    obj.phone1 = data_dict.get("phone1", obj.phone1)
    obj.phone2 = data_dict.get("phone2", obj.phone2)
    obj.region = data_dict.get("region", obj.region)
    obj.area = data_dict.get("area", obj.area)
    obj.commune = data_dict.get("commune", obj.commune)
    obj.city = data_dict.get("city", obj.city)
    obj.latitude = data_dict.get("latitude", obj.latitude)
    obj.longitude = data_dict.get("longitude", obj.longitude)

    # Сохраняем изменения
    await obj.save()


# Excel generation logic moved to services/excel_service.py



@app.post("/delivery/send", dependencies=[Depends(check_not_guest)])
async def send_delivery(data: DeliveryRequest, X_Telegram_Init_Data: str = Header()):
    parsed_init_data = check_telegram_auth(X_Telegram_Init_Data)
    user_info_str = parsed_init_data.get("user")
    user_data = json.loads(user_info_str)
    telegram_id = user_data.get("id")
    # ----------------------------Сообщение для Телеграм-----------------------
    logger.info(X_Telegram_Init_Data)
    message_lines = [
        f"👤 Менеджер: {data.manager}",
        f"🚚 Контрагент: <code>{data.client}</code>",
        f"📍 Адреса: {data.address}",
        f"⚖️ Вага: {data.total_weight} кг",
        f"👤 Контакт: {data.contact}",
        f"📞 Телефон: {data.phone}",
        f"📅 Дата доставки: {data.date}",
        f"💬 Коментар: {data.comment}",
        "",
    ]

    for order in data.orders:
        message_lines.append(f"📦 <b>Замовлення</b> <code>{order.order}</code>")
        message_lines.append("─" * 20)

        for item in order.items:
            message_lines.append(f"🔹 <b>{item.product}</b>")
            message_lines.append(f"   │ <i>Кількість:</i> {item.quantity} шт.")

            active_parties = [p for p in item.parties if p.moved_q > 0]
            count = len(active_parties)

            if count > 0:
                for i, party in enumerate(active_parties):
                    is_last = i == count - 1
                    branch_symbol = "└" if is_last else "├"
                    message_lines.append(
                        f"   {branch_symbol} 🔖 <code>{party.party}</code>: {party.moved_q} шт."
                    )
            else:
                pass
            message_lines.append("")

        message_lines.append("════════════════════")
        message_lines.append("")
    
    message = "\n".join(message_lines)
    # ------------------------------------------------------------------------------

    # --- Генерація та надсилання Excel (винесено в сервіс, тимчасово вимкнено) ---
    # from .services.excel_service import send_delivery_excel_report
    # background_tasks.add_task(send_delivery_excel_report, data, admins)
    # ------------------------------------------------------------------------------

    # Проверяем окружение. Если не 'prod', выводим в консоль вместо отправки.
    app_env = os.getenv("APP_ENV", "dev")

    calendar = await create_calendar_event(data)
    if calendar:
        calendar_link = calendar.get("htmlLink")
        start_info = calendar.get("start", {})
        date_str = start_info.get("date") or start_info.get("dateTime")
        date_val = datetime.fromisoformat(date_str).date()
        await Events.insert(
            Events(
                event_id=calendar["id"],
                event_creator=data.override_created_by if data.override_created_by else telegram_id,
                event_creator_name=data.manager,
                event_status=0,
                start_event=date_val,
                event=data.client,
            )
        ).run()
        logger.info(f"📅 Добавлено в календарь: {calendar_link}")
    else:
        logger.info("❌ Не удалось добавить в календарь")

    # --- Збереження даних в БД ---
    try:
        new_delivery = Deliveries(
            client=data.client,
            manager=data.manager,
            address=data.address,
            contact=data.contact,
            phone=data.phone,
            delivery_date=datetime.strptime(data.date, "%Y-%m-%d").date(),
            comment=data.comment,
            is_custom_address=data.is_custom_address,
            latitude=data.latitude,
            longitude=data.longitude,
            total_weight=data.total_weight,
            status=data.status,
            created_by=data.override_created_by if data.override_created_by else telegram_id,
            calendar_id=calendar["id"] if calendar else None,
        )
        await new_delivery.save().run()
        logger.info(f"✅ Основна інформація по доставці ID: {new_delivery.id} збережена.")

        items_to_insert = []
        for order in data.orders:
            for item in order.items:
                if item.parties:
                    for party in item.parties:
                        if party.moved_q > 0:
                            items_to_insert.append(
                                DeliveryItems(
                                    delivery=new_delivery.id,
                                    order_ref=order.order,
                                    product=item.product,
                                    quantity=item.quantity,
                                    party=party.party,
                                    party_quantity=party.moved_q,
                                )
                            )
        if items_to_insert:
            await DeliveryItems.insert(*items_to_insert).run()
            logger.info(f"✅ {len(items_to_insert)} позицій по доставці збережено.")
            
        await notify_new_delivery(new_delivery, custom_text=message)

    except Exception as e:
        logger.error(f"❌ Помилка збереження доставки в БД: {e}")
        raise HTTPException(status_code=500, detail=f"Помилка збереження в БД: {e}")
    
    return {"status": "ok"}


@app.post("/add_address_for_client", dependencies=[Depends(check_not_guest)])
async def create_address_for_client(address_data: AddressCreate):
    """
    Создает новый адрес для клиента, "умно" разбирая строку полного адреса.
    """
    # 1. Преобразуем входные данные в словарь
    data_dict = address_data.dict()
    full_address_str = data_dict.pop("address", None)

    if not full_address_str:
        raise HTTPException(
            status_code=400, detail="Поле 'full_address' обязательно для заполнения."
        )

    # 2. Разбираем строку адреса на части
    address_parts = [part.strip() for part in full_address_str.split(",")]

    data_dict["region"] = address_parts[3].split()[0]
    data_dict["area"] = address_parts[2].split()[0]
    data_dict["commune"] = address_parts[1].split()[0]
    data_dict["city"] = address_parts[0]

    # 4. Создаем и сохраняем объект ClientAddress с разобранными данными
    try:
        new_address = ClientAddress(**data_dict)
        await new_address.save().run()
        return {"status": "ok", "message": "Адрес успешно создан."}
    except UniqueViolationError:
        raise HTTPException(
            status_code=409,  # 409 Conflict - стандартный код для таких случаев
            detail="Така адреса для цього клієнта вже існує.",
        )
    except Exception as e:
        # Обработка ошибок, если не хватает обязательных полей в ClientAddress
        raise HTTPException(
            status_code=500, detail=f"Ошибка при сохранении адреса: {e}"
        )


@app.get("/addresses/search", response_model=List[AddressResponse])
async def search_addresses(
    q: str = Query(..., min_length=3, description="Название населенного пункта"),
    region_id: str = Query(..., description="ID области (level_1_id)"),
):
    # Ищем только в конкретной области (level_1_id == region_id)
    query = (
        AddressGuide.select(
            AddressGuide.name,
            AddressGuide.category,
            AddressGuide.level_1_id.name.as_alias("region"),
            AddressGuide.level_2_id.name.as_alias("district"),
            AddressGuide.level_3_id.name.as_alias("community"),
        )
        .where(
            AddressGuide.name.ilike(f"%{q}%"),
            AddressGuide.category.is_in(["M", "X", "C"]),  # Только населенные пункты
            AddressGuide.level_1_id == region_id,  # Фильтр по области
        )
        .limit(20)
    )
    results = await query.run()

    response = []
    for row in results:
        parts = [row.get("district"), row.get("community"), row.get("name")]
        full_addr = ", ".join([p for p in parts if p])

        response.append(
            {
                "name": row["name"],
                "category": row["category"],
                "full_address": full_addr,
                "region": row.get("region"),
                "district": row.get("district"),
                "community": row.get("community"),
            }
        )

    return response


@app.get("/delivery/get_telegram_id_from_delivery_by_id/{id}")
async def get_telegram_id(id):
    try:
        telegram_id = (
            await Deliveries.objects().where(Deliveries.calendar_id == str(id)).first()
        )
        return telegram_id.created_by
    except:
        return


@app.get("/delivery/get_data_for_delivery")
async def get_data_for_delivery(X_Telegram_Init_Data: str = Header()):
    parsed_init_data = check_telegram_auth(X_Telegram_Init_Data)
    if not parsed_init_data:
        raise HTTPException(status_code=401, detail="Unauthorized")

    # 1. Получаем все доставки и их товарные позиции
    deliveries_list = (
        await Deliveries.select().order_by(Deliveries.id, ascending=False).run()
    )
    items_list = await DeliveryItems.select().run()

    # 2. Создаем "карту" доставок для быстрой сборки
    deliveries_map = {
        delivery["id"]: {**delivery, "items": []} for delivery in deliveries_list
    }

    # 3. Группируем товарные позиции по доставкам и продуктам
    # { delivery_id: { (order_ref, product_name): { ... } } }
    grouped_items = {}
    for item in items_list:
        delivery_id = item["delivery"]
        product_name = item["product"]
        order_ref = item["order_ref"]
        # Создаем уникальный ключ для группировки по заказу и продукту
        grouping_key = (order_ref, product_name)

        # Инициализируем словари, если их еще нет
        if delivery_id not in grouped_items:
            grouped_items[delivery_id] = {}
        if grouping_key not in grouped_items[delivery_id]:
            grouped_items[delivery_id][grouping_key] = {
                "order_ref": order_ref,  # Возвращаем order_ref
                "product": product_name,
                "quantity": item["quantity"],  # Общее количество для продукта
                "parties": [],
            }

        # Добавляем информацию о партии
        grouped_items[delivery_id][grouping_key]["parties"].append(
            {"party": item["party"], "party_quantity": item["party_quantity"]}
        )

    # 4. Собираем финальный результат
    for delivery_id, delivery_data in deliveries_map.items():
        if delivery_id in grouped_items:
            # Преобразуем словарь продуктов в список
            delivery_data["items"] = list(grouped_items[delivery_id].values())

    combined_data = list(deliveries_map.values())
    return combined_data

@app.post("/delivery/send", dependencies=[Depends(check_not_guest)])
async def send_delivery(
    data: DeliveryRequest, 
    background_tasks: BackgroundTasks,
    X_Telegram_Init_Data: str = Header()
):
    parsed_init_data = check_telegram_auth(X_Telegram_Init_Data)
    user_info_str = parsed_init_data.get("user")
    user_data = json.loads(user_info_str)
    telegram_id = user_data.get("id")

    # 1. Формування повідомлення для Telegram
    message_lines = [
        "🆕 <b>Нова заявка на доставку!</b>",
        "",
        f"👤 Менеджер: {data.manager}",
        f"🚚 Контрагент: <code>{data.client}</code>",
        f"📍 Адреса: {data.address}",
        f"👤 Контакт: {data.contact}",
        f"📞 Телефон: {data.phone}",
        f"📅 Дата доставки: {data.date}",
        f"⚖️ Вага: {data.total_weight} кг",
        f"💬 Коментар: {data.comment}",
        "",
    ]

    for order in data.orders:
        message_lines.append(f"📦 <b>Замовлення</b> <code>{order.order}</code>")
        message_lines.append("─" * 20)

        for item in order.items:
            message_lines.append(f"🔹 <b>{item.product}</b>")
            message_lines.append(f"   │ <i>Кількість:</i> {item.quantity} шт.")

            active_parties = [p for p in item.parties if p.moved_q > 0]
            count = len(active_parties)

            if count > 0:
                for i, party in enumerate(active_parties):
                    is_last = i == count - 1
                    branch_symbol = "└" if is_last else "├"
                    message_lines.append(
                        f"   {branch_symbol} 🔖 <code>{party.party}</code>: {party.moved_q} шт."
                    )
            message_lines.append("")

        message_lines.append("════════════════════")
        message_lines.append("")
    
    message = "\n".join(message_lines)

    # 2. Створення події в календарі
    calendar = await create_calendar_event(data)
    calendar_id = calendar.get("id") if calendar else None
    if calendar:
        logger.info(f"📅 Додано в календарь: {calendar.get('htmlLink')}")
        
        # Збереження події в таблицю Events
        start_info = calendar.get("start", {})
        date_str = start_info.get("date") or start_info.get("dateTime")
        date_val = datetime.fromisoformat(date_str).date()
        
        await Events.insert(
            Events(
                event_id=calendar_id,
                event_creator=data.override_created_by if data.override_created_by else telegram_id,
                event_creator_name=data.manager,
                event_status=0,
                start_event=date_val,
                event=data.client,
            )
        ).run()
    else:
        logger.info("❌ Не удалось добавить в календарь")

    # 3. Збереження даних в БД
    try:
        new_delivery = Deliveries(
            client=data.client,
            manager=data.manager,
            address=data.address,
            contact=data.contact,
            phone=data.phone,
            delivery_date=datetime.strptime(data.date, "%Y-%m-%d").date(),
            comment=data.comment,
            is_custom_address=data.is_custom_address,
            latitude=data.latitude,
            longitude=data.longitude,
            total_weight=data.total_weight,
            status=data.status,
            created_by=data.override_created_by if data.override_created_by else telegram_id,
            calendar_id=calendar_id,
        )
        await new_delivery.save().run()
        logger.info(f"✅ Основна інформація по доставці ID: {new_delivery.id} збережена.")

        items_to_insert = []
        for order in data.orders:
            for item in order.items:
                if item.parties:
                    for party in item.parties:
                        if party.moved_q > 0:
                            items_to_insert.append(
                                DeliveryItems(
                                    delivery=new_delivery.id,
                                    order_ref=order.order,
                                    product=item.product,
                                    quantity=item.quantity,
                                    party=party.party,
                                    party_quantity=party.moved_q,
                                )
                            )
        if items_to_insert:
            await DeliveryItems.insert(*items_to_insert).run()
            logger.info(f"✅ {len(items_to_insert)} позицій по доставці збережено.")
            
        # notify_new_delivery видалено, бо тепер надсилається одне детальне повідомлення

    except Exception as e:
        logger.error(f"❌ Помилка збереження доставки в БД: {e}")
        raise HTTPException(status_code=500, detail=f"Помилка збереження в БД: {e}")

    # 4. Відправка повідомлень адміністраторам, логістам та власнику
    # Отримуємо список адміністраторів з оточення
    admins_json = os.getenv("ADMINS", "[]")
    admins = json.loads(admins_json)
    
    # Об'єднуємо з ідентифікаторами логістів (з конфігу), щоб усі отримували детальне повідомлення
    # Використовуємо set для унікальності
    all_recipients = list(set(admins + LOGISTICS_TELEGRAM_IDS))
    
    # Відправка всім отримувачам (адміни + логісти)
    for recipient_id in all_recipients:
        if SEND_NOTIFICATIONS:
            try:
                await bot.send_message(chat_id=recipient_id, text=message, parse_mode="HTML")
            except Exception as e:
                logger.error(f"❌ Помилка відправки отримувачу {recipient_id}: {e}")
        else:
            logger.info(f"🔇 Сповіщення вимкнено. Дані для {recipient_id} пропущено.")

    # Відправка власнику (якщо його немає в списку отримувачів)
    owner_id = data.override_created_by if data.override_created_by else telegram_id
    if owner_id not in all_recipients:
        if SEND_NOTIFICATIONS:
            try:
                await bot.send_message(chat_id=owner_id, text='<b>Ви успішно зареєстрували доставку:</b>', parse_mode='HTML')
                await bot.send_message(chat_id=owner_id, text=message, parse_mode='HTML')
            except Exception as e:
                logger.error(f'Помилка при сповіщенні власника {owner_id}: {e}')
    
    if telegram_id not in all_recipients and telegram_id != owner_id:
        if SEND_NOTIFICATIONS:
            try:
                await bot.send_message(
                    chat_id=telegram_id, 
                    text='✅ Ви успішно зареєстрували доставку. Дякуємо за роботу!'
                )
            except Exception as e:
                logger.error(f'Помилка при сповіщенні ініціатора {telegram_id}: {e}')
    
    return {"status": "ok", "id": new_delivery.id}


@app.post("/delivery/update", dependencies=[Depends(check_not_guest)])
async def update_delivery(
    data: UpdateDeliveryRequest,
    X_Telegram_Init_Data: str = Header()
):
    """
    Оновлення доставки: статус, вага та склад (позиції/партії).
    Збирає попередження (warnings), якщо виникли проблеми з Telegram або Календарем, 
    але продовжує виконання основної логіки БД.
    """
    parsed_init_data = check_telegram_auth(X_Telegram_Init_Data)
    if not parsed_init_data:
        raise HTTPException(status_code=401, detail="Unauthorized")

    warnings = []

    try:
        # 1. Отримуємо існуючу доставку
        delivery_data = await Deliveries.objects().where(Deliveries.id == data.delivery_id).first().run()
        if not delivery_data:
            raise HTTPException(status_code=404, detail="Delivery not found")

        # 2. Оновлюємо статус, якщо змінився
        if delivery_data.status != data.status:
            old_status = delivery_data.status
            delivery_data.status = data.status
            
            # Повідомлення про зміну статусу
            try:
                await notify_delivery_status_change(
                    delivery=delivery_data, 
                    status=data.status, 
                    actor_name=data.actor_name,
                    actor_id=parsed_init_data["id"]
                )
            except Exception as e:
                logger.error(f"Error notifying status change: {e}")
                warnings.append(f"Помилка сповіщення Telegram: {e}")

            # Оновлення в календарі
            if delivery_data.calendar_id:
                try:
                    cal_status = 2 if data.status == "Виконано" else 1
                    changed_color_calendar_events_by_id(event_id=delivery_data.calendar_id, status_code=cal_status)
                    await Events.update({Events.event_status: cal_status}).where(
                        Events.event_id == delivery_data.calendar_id
                    ).run()
                except Exception as e:
                    logger.error(f"Error updating calendar color: {e}")
                    warnings.append(f"Помилка оновлення Календаря: {e}")

            # Додаткові сповіщення менеджеру при певних статусах
            if data.status == 'Виконано':
                if delivery_data.created_by:
                    try:
                        await bot.send_message(
                            chat_id=delivery_data.created_by,
                            text=(
                                f"✅ <b>Доставка завершена</b>\n\n"
                                f"👤 Клієнт: <b>{delivery_data.client}</b>\n"
                            ),
                            parse_mode="HTML",
                        )
                    except Exception as tg_err:
                        logger.warning(f"Error sending completion message: {tg_err}")

            elif data.status == 'В очікуванні':
                if delivery_data.created_by:
                    try:
                        await bot.send_message(
                            chat_id=delivery_data.created_by,
                            text=(
                                f"⏳ <b>Доставка в очікуванні</b>\n\n"
                                f"👤 Клієнт: <b>{delivery_data.client}</b>\n"
                                f"📅 Очікувана дата: <b>{delivery_data.delivery_date}</b>\n\n"
                                f"Коли продукція буде готова до відвантаження, ви отримаєте ще одне повідомлення.\n"),
                            parse_mode="HTML",
                        )
                    except Exception as tg_err:
                        logger.warning(f"Error sending waiting message: {tg_err}")

            elif data.status == 'Продукція готова до відвантаження':
                if delivery_data.created_by:
                    items_text = "\n".join([f"🔹 {item.product}: <b>{item.quantity}</b>" for item in data.items])
                    try:
                        await bot.send_message(
                            chat_id=delivery_data.created_by,
                            text=(
                                f"📦 <b>Продукція готова до відвантаження</b>\n\n"
                                f"👤 Клієнт: <b>{delivery_data.client}</b>\n"
                                f"📦 Склад:\n{items_text}\n\n"
                                f"<i>Підтвердіть дату та час з логістом.</i>\n"),
                            parse_mode="HTML",
                        )
                    except Exception as tg_err:
                        logger.warning(f"Error sending ready message: {tg_err}")

        # 3. Оновлюємо вагу та зберігаємо зміни доставки
        if data.total_weight is not None:
            delivery_data.total_weight = data.total_weight
        
        await delivery_data.save().run()

        # 4. Оновлюємо склад доставки (позиції та партії)
        async with DeliveryItems._meta.db.transaction():
            await DeliveryItems.delete().where(
                DeliveryItems.delivery == data.delivery_id
            ).run()

            items_to_insert = []
            for item in data.items:
                if item.parties:
                    for party in item.parties:
                        if party.moved_q > 0:
                            items_to_insert.append(
                                DeliveryItems(
                                    delivery=data.delivery_id,
                                    order_ref=item.order_ref,
                                    product=item.product,
                                    quantity=item.quantity,
                                    party=party.party,
                                    party_quantity=party.moved_q,
                                )
                            )
                else:
                    items_to_insert.append(
                        DeliveryItems(
                            delivery=data.delivery_id,
                            order_ref=item.order_ref,
                            product=item.product,
                            quantity=item.quantity,
                        )
                    )

            if items_to_insert:
                await DeliveryItems.insert(*items_to_insert).run()
            else:
                await Deliveries.delete().where(Deliveries.id == data.delivery_id).run()
                return {
                    "status": "ok",
                    "message": "Delivery deleted as it became empty.",
                    "warnings": warnings
                }

        return {
            "status": "ok", 
            "message": "Delivery updated successfully.", 
            "warnings": warnings
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in update_delivery: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Помилка при оновленні доставки: {e}",
        )

async def update_delivery_date(
    data: ChangeDeliveryDateRequest,
    X_Telegram_Init_Data: str = Header()
):
    """
    Оновлює дату доставки, оновлює подію в Google Calendar та відправляє повідомлення менеджеру.
    """
    parsed_init_data = check_telegram_auth(X_Telegram_Init_Data)
    if not parsed_init_data:
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        # 1. Знаходимо доставку
        delivery = await Deliveries.objects().where(Deliveries.id == data.delivery_id).first()
        if not delivery:
            raise HTTPException(status_code=404, detail="Delivery not found")

        old_date = delivery.delivery_date
        new_date_obj = datetime.strptime(data.new_date, "%Y-%m-%d").date()

        if old_date == new_date_obj:
            return {"status": "ok", "message": "Date is unchanged."}

        # 2. Оновлюємо дату в базі
        delivery.delivery_date = new_date_obj
        await delivery.save().run()

        # 3. Оновлюємо подію в Google Calendar та таблиці Events
        if delivery.calendar_id:
            changed_date_calendar_events_by_id(delivery.calendar_id, new_date_obj)
            await Events.update({Events.start_event: new_date_obj}).where(
                Events.event_id == delivery.calendar_id
            ).run()

        # 4. Відправляємо повідомлення в Telegram
        manager_id = delivery.created_by
        if manager_id:
            message_text = (
                f"📅 <b>Увага!</b> Змінено дату доставки.\n\n"
                f"👤 Клієнт: <b>{delivery.client}</b>\n"
                f"🗓 Стара дата: {old_date}\n"
                f"🆕 <b>Нова дата: {new_date_obj}</b>"
            )
            await bot.send_message(
                chat_id=manager_id,
                text=message_text,
                parse_mode="HTML"
            )

        logger.info(f"✅ Дата доставки ID: {data.delivery_id} оновлена з {old_date} на {new_date_obj}.")
        return {"status": "ok", "message": "Delivery date updated successfully."}

    except Exception as e:
        logger.info(f"❌ Помилка оновлення дати доставки: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Не вдалося оновити дату доставки: {e}",
        )


@app.post("/delivery/batch_update", tags=["Delivery"], dependencies=[Depends(check_not_guest)])
async def batch_update_deliveries(
    data: BatchUpdateDeliveryRequest,
    X_Telegram_Init_Data: str = Header()
):
    """
    Масове оновлення статусу або дати для списку доставок.
    Групує сповіщення по менеджерах.
    """
    parsed_init_data = check_telegram_auth(X_Telegram_Init_Data)
    if not parsed_init_data:
        raise HTTPException(status_code=401, detail="Unauthorized")

    if not data.delivery_ids:
        return {"status": "ok", "message": "No deliveries to update."}

    try:
        # 1. Отримуємо всі доставки для оновлення
        deliveries_to_update = await Deliveries.objects().where(
            Deliveries.id.is_in(data.delivery_ids)
        ).run()

        if not deliveries_to_update:
            return {"status": "ok", "message": "No matching deliveries found."}

        # 2. Оновлення в базі та підготовка даних для сповіщень
        # { manager_id: [delivery_info, ...] }
        grouped_by_manager = {}

        async with Deliveries._meta.db.transaction():
            for delivery in deliveries_to_update:
                changes = []
                
                # Оновлення статусу
                if data.status and delivery.status != data.status:
                    old_status = delivery.status
                    delivery.status = data.status
                    changes.append(f"статус: {old_status} ➔ <b>{data.status}</b>")
                    
                    # Google Calendar color update
                    if delivery.calendar_id:
                        cal_status = 2 if data.status == "Виконано" else 1
                        changed_color_calendar_events_by_id(event_id=delivery.calendar_id, status_code=cal_status)
                        await Events.update({Events.event_status: cal_status}).where(
                            Events.event_id == delivery.calendar_id
                        ).run()

                # Оновлення дати
                if data.new_date:
                    new_date_obj = datetime.strptime(data.new_date, "%Y-%m-%d").date()
                    if delivery.delivery_date != new_date_obj:
                        old_date = delivery.delivery_date
                        delivery.delivery_date = new_date_obj
                        changes.append(f"дата: {old_date} ➔ <b>{new_date_obj}</b>")
                        
                        # Google Calendar date update
                        if delivery.calendar_id:
                            changed_date_calendar_events_by_id(delivery.calendar_id, new_date_obj)
                            await Events.update({Events.start_event: new_date_obj}).where(
                                Events.event_id == delivery.calendar_id
                            ).run()

                if changes:
                    await delivery.save().run()
                    
                    manager_id = delivery.created_by
                    if manager_id:
                        if manager_id not in grouped_by_manager:
                            grouped_by_manager[manager_id] = []
                        grouped_by_manager[manager_id].append({
                            "id": delivery.id,
                            "client": delivery.client,
                            "changes": changes
                        })

        # 3. Відправка згрупованих сповіщень
        for manager_id, items in grouped_by_manager.items():
            message_lines = [f"🔄 <b>Пакетне оновлення доставок ({len(items)})</b>\n"]
            
            for item in items:
                changes_str = ", ".join(item["changes"])
                message_lines.append(f"📦 <b>{item['client']}</b>")
                message_lines.append(f"└ {changes_str}\n")
            
            await bot.send_message(
                chat_id=manager_id,
                text="\n".join(message_lines),
                parse_mode="HTML"
            )

        logger.info(f"✅ Успішно оновлено {len(deliveries_to_update)} доставок пакетно.")
        return {"status": "ok", "message": f"Successfully updated {len(deliveries_to_update)} deliveries."}

    except Exception as e:
        logger.info(f"❌ Помилка пакетного оновлення доставок: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Не вдалося оновити доставки пакетно: {e}",
        )


@app.post(
    "/orders/comments/create",
    response_model=CommentResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Створити коментар",
    description="Створює новий коментар до заявки або товару",
    dependencies=[Depends(check_not_guest)],
)
async def create_comment(
    request: CreateCommentRequest, user: dict = Depends(get_current_telegram_user)
):
    """
    Створення нового коментаря

    - **comment_type**: 'order' для заявки, 'product' для товару
    - **order_ref**: Номер заявки (обов'язково)
    - **product_id**: UUID товару (для дашборду, якщо comment_type='product')
    - **product_name**: Назва товару (для BI, якщо comment_type='product')
    - **comment_text**: Текст коментаря (обов'язково)
    """

    try:
        # Создаем новую запись в таблице OrderComments
        new_comment = OrderComments(
            comment_type=request.comment_type.value,
            order_ref=request.order_ref,
            product_id=request.product_id,
            product_name=request.product_name,
            comment_text=request.comment_text,
            created_by=user["telegram_id"],
            created_by_name=user["full_name_for_orders"] or user["first_name"],
        )
        await new_comment.save().run()

        # Возвращаем созданный объект, преобразованный в Pydantic модель
        return CommentResponse(
            id=str(new_comment.id),
            comment_type=new_comment.comment_type,
            order_ref=new_comment.order_ref,
            product_id=str(new_comment.product_id) if new_comment.product_id else None,
            product_name=new_comment.product_name,
            comment_text=new_comment.comment_text,
            created_by=new_comment.created_by,
            created_by_name=new_comment.created_by_name,
            created_at=new_comment.created_at,
            updated_at=new_comment.updated_at,
        )
    except Exception as e:
        logger.info(f"❌ Помилка створення коментаря: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Не вдалося зберегти коментар: {e}",
        )


@app.api_route(
    "/orders/comments/list",
    methods=["GET", "POST"],
    response_model=List[CommentResponse],
    summary="Отримати коментарі",
    description="Отримує всі коментарі для вказаної заявки (GET через query або POST через JSON body)",
)
async def get_comments(
    request: Request,
    order_ref: Optional[List[str]] = Query(None, description="Номер заявки (для GET)"),
):
    """
    Отримання всіх коментарів для заявки або списку заявок.
    Підтримує GET з параметрами в URL та POST з JSON списком ["ID1", "ID2", ...].
    """
    refs = order_ref or []

    if request.method == "POST":
        try:
            body = await request.json()
            if isinstance(body, list):
                refs = body
            elif isinstance(body, dict) and "order_ref" in body:
                refs = body["order_ref"]
                if not isinstance(refs, list):
                    refs = [refs]
        except Exception:
            pass

    if not refs:
        return []

    comments = (
        await OrderComments.select()
        .where(OrderComments.order_ref.is_in(refs))
        .order_by(OrderComments.created_at, ascending=False)
        .run()
    )

    return comments


@app.put(
    "/orders/comments/{comment_id}",
    response_model=CommentResponse,
    summary="Оновити коментар",
    description="Оновлює текст коментаря (тільки власник може редагувати)",
    dependencies=[Depends(check_not_guest)],
)
async def update_comment(
    comment_id: int,
    request: UpdateCommentRequest,
    user: dict = Depends(get_current_telegram_user),
):
    """
    Оновлення коментаря

    - **comment_id**: ID коментаря
    - **comment_text**: Новий текст коментаря

    Тільки автор коментаря може його редагувати
    """

    # Перевірка існування та прав доступу
    comment = (
        await OrderComments.objects()
        .where(OrderComments.id == comment_id)
        .first()
        .run()
    )

    if not comment:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Коментар не знайдено"
        )

    if comment.created_by != user["telegram_id"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Ви можете редагувати тільки свої коментарі",
        )

    # Оновлення
    comment.comment_text = request.comment_text
    comment.updated_at = datetime.now()
    await comment.save().run()

    return CommentResponse(
        id=str(comment.id),
        comment_type=comment.comment_type,
        order_ref=comment.order_ref,
        product_id=str(comment.product_id) if comment.product_id else None,
        product_name=comment.product_name,
        comment_text=comment.comment_text,
        created_by=comment.created_by,
        created_by_name=comment.created_by_name,
        created_at=comment.created_at,
        updated_at=comment.updated_at,
    )


@app.delete(
    "/orders/comments/{comment_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Видалити коментар",
    description="Видаляє коментар (тільки власник може видалити)",
    dependencies=[Depends(check_not_guest)],
)
async def delete_comment(
    comment_id: int, user: dict = Depends(get_current_telegram_user)
):
    """
    Видалення коментаря

    - **comment_id**: ID коментаря

    Тільки автор коментаря може його видалити
    """

    # Перевірка існування та прав доступу
    comment = (
        await OrderComments.objects()
        .where(OrderComments.id == comment_id)
        .first()
        .run()
    )

    if not comment:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Коментар не знайдено"
        )

    if comment.created_by != user["telegram_id"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Ви можете видаляти тільки свої коментарі",
        )

    # Видалення
    await OrderComments.delete().where(OrderComments.id == comment_id).run()

    return None
