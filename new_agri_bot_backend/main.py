from __future__ import annotations
import csv
import html
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
from .exceptions import ExcelValidationError
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
    MapLoBRequest,
    ChangeDeliveryDateRequest,
    RequestNPDetailsRequest,
    CreateCommentRequest,
    UpdateCommentRequest,
    CommentResponse,
    CommentType,
    ClientData,
    Order,
    Product,
    SplitDeliveryRequest,
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
    ValidWarehouseAdmin,
    Users,
    ClientManagerGuide,
    DetailsForOrders,
    Accountants,
    ManagerAccountantGuide,
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
    WebSocket,
    WebSocketDisconnect,
)
from .websocket_manager import manager
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
from .cache import cached_endpoint, db_cache
from .bi import router as bi_router
from .bi_pandas import router as bi_pandas_router
from .order_chat import router as chat_router
from .notification import router as notification_router
from .nova_poshta import router as nova_poshta_router, call_np_api
from .bot_handlers import setup_bot_handlers
from .scheduler import setup_scheduler
from .utils import send_message_to_managers, create_composite_key_from_dict
from .delivery_notifications import (
    notify_new_delivery, 
    notify_delivery_status_change, 
    delete_delivery_notifications, 
    notify_delivery_date_change, 
    ALL_RECIPIENTS,
    notify_request_np_details_to_manager,
    notify_np_details_filled
)
from .error_notifier import notify_admins_error

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





from urllib.parse import urlparse
admin_allowed_hosts = list({urlparse(origin).hostname for origin in CORS_ORIGINS if urlparse(origin).hostname})
if "localhost" not in admin_allowed_hosts: admin_allowed_hosts.append("localhost")
if "127.0.0.1" not in admin_allowed_hosts: admin_allowed_hosts.append("127.0.0.1")

admin_router = create_admin(
    [Remains, ValidWarehouseAdmin, Users, Accountants, ManagerAccountantGuide],
    allowed_hosts=admin_allowed_hosts
)

from .models import SendToAccountantRequest
from .services.accountant_service import (
    send_accountant_telegram,
    send_accountant_email,
    generate_printable_html,
    generate_mailto_url,
)


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
        if "(1500К)" in nomenclature:
            return 8.0
        if "(150К)" in nomenclature:
            return 10.0
        if "(50К)" in nomenclature:
            return 15.0
        if "(80К)" in nomenclature:
            return 20.0

    # Если ни одно из правил не подошло, возвращаем 1.0
    return 1.0


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



# Глобальный манки-патч для Starlette Request.form, чтобы увеличить лимит размера части (max_part_size) с 1 МБ до 100 МБ.
# Это позволяет загружать большие JSON-строки в параметре manual_matches_json без ошибок 400 Bad Request.
from starlette.requests import Request as StarletteRequest

original_form = StarletteRequest.form

async def custom_form(
    self: StarletteRequest,
    *,
    max_files: int | float = 1000,
    max_fields: int | float = 1000,
    max_part_size: int = 100 * 1024 * 1024,  # 100 МБ
) -> any:
    return await original_form(
        self,
        max_files=max_files,
        max_fields=max_fields,
        max_part_size=max_part_size,
    )

StarletteRequest.form = custom_form


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
    max_age=600,
)


from fastapi.exceptions import RequestValidationError


# --- Глобальный перехватчик ошибок: отправляет уведомление в Telegram ---
@app.middleware("http")
async def error_notify_middleware(request: Request, call_next):
    """Middleware: перехватывает все 5xx ответы и уведомляет администраторов."""
    try:
        response = await call_next(request)
        return response
    except Exception as exc:
        logger.error(
            f"Unhandled exception on {request.method} {request.url.path}: {exc}",
            exc_info=True,
        )
        # Отправляем уведомление в Telegram (fire-and-forget)
        asyncio.create_task(
            notify_admins_error(
                exc,
                path=request.url.path,
                method=request.method,
            )
        )
        return JSONResponse(
            status_code=500,
            content={"detail": "Internal server error"},
        )


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Перехватчик всех HTTPException (400, 401, 403, 404, 500) — логирует и шлёт в Telegram при 400+."""
    logger.error(
        f"HTTPException {exc.status_code} on {request.method} {request.url.path}: {exc.detail}"
    )
    if exc.status_code >= 400:
        asyncio.create_task(
            notify_admins_error(
                exc,
                path=request.url.path,
                method=request.method,
                extra=f"Status: {exc.status_code}, Detail: {exc.detail}",
            )
        )
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail},
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Перехватчик ошибок валидации Pydantic — уведомляет Telegram с деталями."""
    errors_summary = json.dumps(exc.errors(), ensure_ascii=False)
    logger.error(
        f"Validation error on {request.method} {request.url.path}: {errors_summary}"
    )
    asyncio.create_task(
        notify_admins_error(
            exc,
            path=request.url.path,
            method=request.method,
            extra=f"Validation errors: {errors_summary}",
        )
    )
    return JSONResponse(
        status_code=400,
        content={"detail": exc.errors()},
    )


@app.exception_handler(500)
async def internal_server_error_handler(request: Request, exc: Exception):
    """Глобальный обработчик 500 ошибок — уведомляет Telegram."""
    logger.error(
        f"500 error on {request.method} {request.url.path}: {exc}",
        exc_info=True,
    )
    asyncio.create_task(
        notify_admins_error(
            exc,
            path=request.url.path,
            method=request.method,
        )
    )
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"},
    )


@app.get("/health", tags=["System"])
async def health_check():
    """Перевірка працездатності сервісу."""
    return {
        "status": "ok",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "version": "1.0.0"
    }

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    # Добавляем логирование для отладки
    client_host = websocket.client.host if websocket.client else "unknown"
    logger.info(f"🔍 Попытка WebSocket подключения с хоста: {client_host}")
    
    try:
        await manager.connect(websocket)
        logger.info(f"✅ WebSocket соединение успешно установлено для: {client_host}")
        
        while True:
            # Ожидаем данных, чтобы соединение не закрывалось (keep-alive)
            await websocket.receive_text()
    except WebSocketDisconnect:
        logger.info(f"🔌 WebSocket соединение закрыто клиентом: {client_host}")
        manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"❌ Ошибка в WebSocket для {client_host}: {e}")
        manager.disconnect(websocket)

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
    except ExcelValidationError as e:
        raise HTTPException(
            status_code=400,
            detail={
                "error_type": "validation_error",
                "file": e.file_type,
                "message": e.message,
                "missing_columns": e.missing_columns
            }
        )
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail={
                "error_type": "unexpected_error",
                "message": f"Непредвиденная ошибка при обработке файлов: {e}"
            }
        )

    # --- НОВОВВЕДЕНИЕ: Фильтрация уже сопоставленных данных ---
    try:
        # 1. Получаем все ранее сопоставленные записи из БД
        existing_moved_records = await MovedData.select(
            MovedData.order, MovedData.product, MovedData.party_sign, MovedData.qt_moved
        ).run()

        # 2. Создаем множество уникальных ключей для быстрой проверки
        existing_keys: set[str] = {
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
@cached_endpoint()
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

    # Piccolo иногда возвращает JSONB как строку — нормализуем
    for addr in address:
        np = addr.get("default_np_data")
        if isinstance(np, str):
            try:
                addr["default_np_data"] = json.loads(np)
            except Exception:
                addr["default_np_data"] = None

    return orders_list, address


@app.get("/get_all_addresses")
async def get_all_addresses():
    address = await ClientAddress.select().run()
    for addr in address:
        np = addr.get("default_np_data")
        if isinstance(np, str):
            try:
                addr["default_np_data"] = json.loads(np)
            except Exception:
                addr["default_np_data"] = None
    return address


@app.get("/get_address_by_client/{client}")
async def get_address_by_client(client):
    address = await ClientAddress.select().where(ClientAddress.client == client).run()
    for addr in address:
        np = addr.get("default_np_data")
        if isinstance(np, str):
            try:
                addr["default_np_data"] = json.loads(np)
            except Exception:
                addr["default_np_data"] = None
    return address


@app.put("/update_address_for_client/{id}", dependencies=[Depends(check_not_guest)])
async def update_address_for_client(address_data: AddressCreate, id: int, request: Request):
    logger.info(f"update_address_for_client id={id}, data={address_data.dict()}")
    try:
        obj = await ClientAddress.objects().get(where=(ClientAddress.id == id))
        data_dict = address_data.dict()
        full_address_str = data_dict.pop("address", None) or data_dict.pop("full_address", None)
        
        if full_address_str:
            # 2. Разбираем строку адреса на части
            address_parts = [part.strip() for part in full_address_str.split(",")]
            
            if len(address_parts) >= 4:
                data_dict["region"] = address_parts[0].split()[0] if address_parts[0] else ""
                data_dict["area"] = address_parts[1].split()[0] if address_parts[1] else ""
                data_dict["commune"] = address_parts[2].split()[0] if address_parts[2] else ""
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
        # Дані авто/водія за замовчуванням
        obj.default_car_make = data_dict.get("default_car_make") or None
        obj.default_car_number = data_dict.get("default_car_number") or None
        obj.default_trailer_number = data_dict.get("default_trailer_number") or None
        obj.default_driver = data_dict.get("default_driver") or None
        # Весогабаритные характеристики
        obj.default_car_max_weight = data_dict.get("default_car_max_weight") or None
        obj.default_car_own_weight = data_dict.get("default_car_own_weight") or None
        obj.default_car_length = data_dict.get("default_car_length") or None
        obj.default_car_width = data_dict.get("default_car_width") or None
        obj.default_car_height = data_dict.get("default_car_height") or None
        obj.default_np_data = data_dict.get("default_np_data") or None

        # Сохраняем изменения
        await obj.save()
        db_cache.clear()
        logger.info(f"update_address_for_client id={id}: saved successfully")
        return {"status": "ok"}
    except Exception as exc:
        logger.error(f"update_address_for_client id={id} error: {exc}", exc_info=True)
        asyncio.create_task(
            notify_admins_error(
                exc,
                path=request.url.path,
                method=request.method,
                extra=f"id={id}, data={address_data.dict()}",
            )
        )
        raise HTTPException(status_code=500, detail=f"Помилка збереження: {str(exc)}")



# Excel generation logic moved to services/excel_service.py




@app.post("/add_address_for_client", dependencies=[Depends(check_not_guest)])
async def create_address_for_client(address_data: AddressCreate, request: Request):
    """
    Создает новый адрес для клиента, "умно" разбирая строку полного адреса.
    """
    data_dict = address_data.dict()
    full_address_str = data_dict.pop("address", None) or data_dict.pop("full_address", None)
    data_dict.pop("full_address", None)

    if full_address_str:
        address_parts = [part.strip() for part in full_address_str.split(",")]

        if len(address_parts) >= 4:
            data_dict["region"] = address_parts[0].split()[0] if address_parts[0] else ""
            data_dict["area"] = address_parts[1].split()[0] if address_parts[1] else ""
            data_dict["commune"] = address_parts[2].split()[0] if address_parts[2] else ""
            data_dict["city"] = address_parts[3]

    # Гарантируем, что обязательные не-null поля имеют значение
    data_dict.setdefault("region", "")
    data_dict.setdefault("area", "")
    data_dict.setdefault("commune", "")
    data_dict.setdefault("city", full_address_str or "")
    data_dict.setdefault("representative", "")
    data_dict.setdefault("phone1", "")
    data_dict.setdefault("phone2", "")

    # Очищаємо порожні рядки для nullable полів авто/водія
    for field in (
        "default_car_make", "default_car_number", "default_trailer_number", "default_driver",
        "default_car_max_weight", "default_car_own_weight", "default_car_length", "default_car_width", "default_car_height"
    ):
        if field in data_dict and not data_dict[field]:
            data_dict[field] = None

    try:
        valid_columns = {col._meta.name for col in ClientAddress._meta.columns}
        clean_dict = {k: v for k, v in data_dict.items() if k in valid_columns}
        new_address = ClientAddress(**clean_dict)
        await new_address.save().run()
        db_cache.clear()
        return {"status": "ok", "message": "Адрес успешно создан."}
    except UniqueViolationError:
        raise HTTPException(
            status_code=409,
            detail="Така адреса для цього клієнта вже існує.",
        )
    except Exception as e:
        logger.error(f"create_address_for_client error: {e}", exc_info=True)
        asyncio.create_task(
            notify_admins_error(
                e,
                path=request.url.path,
                method=request.method,
                extra=f"data={address_data.dict()}",
            )
        )
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


@app.get("/api/vehicle-info/{number}")
async def get_vehicle_info(number: str):
    clean_number = number.upper().replace(" ", "")
    # Ограничим длину номера для безопасности
    if len(clean_number) < 3 or len(clean_number) > 20:
        raise HTTPException(status_code=400, detail="Некоректний формат номера авто")
    
    # 1. Попытка получить данные из публичного API Polis.ua (без авторизации)
    try:
        async with httpx.AsyncClient() as client:
            url = f"https://api.polis.ua/api/osgpo/auto-info/find/v2/{clean_number}"
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Referer": "https://polis.ua/",
                "Accept": "application/json"
            }
            response = await client.get(url, headers=headers, timeout=3.0)
            if response.status_code == 200:
                data = response.json()
                make = data.get("modelText") or data.get("name") or data.get("model") or ""
                if not make and data.get("markName"):
                    make = f"{data.get('markName')} {data.get('modelName', '')}".strip()
                
                max_weight = data.get("weight") or data.get("maxWeight") or data.get("totalWeight")
                if isinstance(max_weight, str) and max_weight.isdigit():
                    max_weight = int(max_weight)
                elif isinstance(max_weight, float):
                    max_weight = int(max_weight)
                
                own_weight = data.get("ownWeight") or data.get("emptyWeight")
                if isinstance(own_weight, str) and own_weight.isdigit():
                    own_weight = int(own_weight)
                elif isinstance(own_weight, float):
                    own_weight = int(own_weight)
                
                length = None
                width = None
                height = None
                
                if not max_weight:
                    car_type_code = str(data.get("carTypeCode") or "").upper()
                    if "C" in car_type_code or "ГРУЗ" in str(data.get("carType", {}).get("name", "")).upper():
                        max_weight = 12000
                        own_weight = 6000
                        length = 7.5
                        width = 2.45
                        height = 3.4
                    else:
                        max_weight = 2200
                        own_weight = 1500
                        length = 4.8
                        width = 1.8
                        height = 1.5
                else:
                    if max_weight > 7500:
                        own_weight = own_weight or int(max_weight * 0.45)
                        length = 8.5
                        width = 2.5
                        height = 3.6
                    elif max_weight > 3500:
                        own_weight = own_weight or int(max_weight * 0.55)
                        length = 6.5
                        width = 2.2
                        height = 2.8
                    else:
                        own_weight = own_weight or int(max_weight * 0.7)
                        length = 4.7
                        width = 1.8
                        height = 1.5
                
                if make:
                    return {
                        "status": "ok",
                        "source": "api",
                        "make": make,
                        "number": clean_number,
                        "max_weight": max_weight,
                        "own_weight": own_weight,
                        "length": length,
                        "width": width,
                        "height": height
                    }
    except Exception as e:
        logger.info(f"--- Ошибка запроса авто через API Polis.ua: {e} ---")

    # 2. Резервный mock-генератор
    digits = [int(c) for c in clean_number if c.isdigit()]
    digit_sum = sum(digits) if digits else 0
    
    if digit_sum % 3 == 0:
        return {
            "status": "ok",
            "source": "mock_heavy",
            "make": "MAN TGS 18.400",
            "number": clean_number,
            "max_weight": 18000,
            "own_weight": 8500,
            "length": 8.2,
            "width": 2.5,
            "height": 3.6
        }
    elif digit_sum % 3 == 1:
        return {
            "status": "ok",
            "source": "mock_medium",
            "make": "Mercedes-Benz Sprinter 316",
            "number": clean_number,
            "max_weight": 3500,
            "own_weight": 2200,
            "length": 5.9,
            "width": 2.0,
            "height": 2.4
        }
    else:
        return {
            "status": "ok",
            "source": "mock_light",
            "make": "Volkswagen Caddy 2.0 TDI",
            "number": clean_number,
            "max_weight": 2200,
            "own_weight": 1450,
            "length": 4.4,
            "width": 1.8,
            "height": 1.8
        }


@app.get("/delivery/get_telegram_id_from_delivery_by_id/{id}")
async def get_telegram_id(id):
    try:
        telegram_id = (
            await Deliveries.objects().where(Deliveries.calendar_id == str(id)).first()
        )
        return telegram_id.created_by
    except:
        return


@app.get("/delivery/get/{id}")
async def get_delivery_by_id(id: int, X_Telegram_Init_Data: str = Header()):
    parsed_init_data = check_telegram_auth(X_Telegram_Init_Data)
    if not parsed_init_data:
        raise HTTPException(status_code=401, detail="Unauthorized")

    delivery = await Deliveries.select().where(Deliveries.id == id).first().run()
    if not delivery:
        raise HTTPException(status_code=404, detail="Delivery not found")

    items_list = await DeliveryItems.select().where(DeliveryItems.delivery == id).run()
    grouped_items = {}
    for item in items_list:
        grouping_key = (item.get("order_ref"), item.get("product"))
        if grouping_key not in grouped_items:
            grouped_items[grouping_key] = {
                "order_ref": item.get("order_ref"),
                "product": item.get("product"),
                "line_of_business": item.get("line_of_business"),
                "quantity": item.get("quantity"),
                "parties": [],
            }
        grouped_items[grouping_key]["parties"].append(
            {
                "party": item.get("party"),
                "party_quantity": item.get("party_quantity"),
                "warehouse": item.get("warehouse"),
            }
        )

    delivery["items"] = list(grouped_items.values())

    client_address = await ClientAddress.select().where(
        ClientAddress.client == delivery["client"]
    ).first().run()
    default_np_data = client_address.get("default_np_data") if client_address else None

    return {
        "delivery": delivery,
        "default_np_data": default_np_data,
        "client_address": client_address
    }



@app.post("/delivery/request_np_details", dependencies=[Depends(check_not_guest)])
async def request_np_details(
    data: RequestNPDetailsRequest,
    X_Telegram_Init_Data: str = Header()
):
    parsed_init_data = check_telegram_auth(X_Telegram_Init_Data)
    if not parsed_init_data:
        raise HTTPException(status_code=401, detail="Unauthorized")

    delivery = await Deliveries.objects().where(Deliveries.id == data.delivery_id).first().run()
    if not delivery:
        raise HTTPException(status_code=404, detail="Delivery not found")

    delivery.status = "Потрібні дані НП"

    user_name = "Логіст"
    user_id = None
    user_data_json = parsed_init_data.get("user")
    if user_data_json:
        try:
            user_obj = json.loads(user_data_json)
            user_id = user_obj.get("id")
            first_name = user_obj.get("first_name", "")
            last_name = user_obj.get("last_name", "")
            full_name = f"{first_name} {last_name}".strip()
            if full_name:
                user_name = full_name
        except Exception:
            pass

    if data.comment and data.comment.strip():
        note = f"\n[Логіст {user_name}]: {data.comment.strip()}"
        delivery.comment = (delivery.comment or "") + note

    await delivery.save().run()

    # Сповіщення менеджеру з WebApp-кнопкою
    if delivery.created_by:
        try:
            await notify_request_np_details_to_manager(delivery, data.comment)
        except Exception as e:
            logger.error(f"Помилка надсилання сповіщення менеджеру по НП: {e}")

    # Сповіщення іншим логістам про зміну статусу
    try:
        await notify_delivery_status_change(
            delivery=delivery,
            status="Потрібні дані НП",
            actor_name=user_name,
            actor_id=user_id
        )
    except Exception as e:
        logger.error(f"Помилка сповіщення логістів про статус 'Потрібні дані НП': {e}")

    return {"status": "success", "message": "Запит успішно надіслано менеджеру"}


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
                "line_of_business": item.get("line_of_business"),
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

@app.get("/delivery/unmapped_lobs")
async def get_unmapped_lobs(X_Telegram_Init_Data: str = Header()):
    parsed_init_data = check_telegram_auth(X_Telegram_Init_Data)
    if not parsed_init_data:
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    # Отримуємо унікальні продукти, де line_of_business IS NULL або пустий
    items = await DeliveryItems.select(DeliveryItems.product).where(
        (DeliveryItems.line_of_business == None) | (DeliveryItems.line_of_business == "")
    ).distinct().run()
    
    unmapped_products = [item["product"] for item in items if item["product"]]
    return unmapped_products

@app.post("/delivery/map_lobs")
async def map_lobs(data: MapLoBRequest, X_Telegram_Init_Data: str = Header()):
    parsed_init_data = check_telegram_auth(X_Telegram_Init_Data)
    if not parsed_init_data:
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    updated_count = 0
    for product_name, lob in data.mappings.items():
        if product_name and lob:
            res = await DeliveryItems.update({
                DeliveryItems.line_of_business: lob
            }).where(DeliveryItems.product == product_name).run()
            # res is usually a list of dicts with updated IDs in Piccolo if returning is used, 
            # but we just count mappings updated as a batch
            updated_count += 1
            
    return {"message": "Успішно оновлено", "updated_products": updated_count}

async def resolve_client_manager_user(client_name: str, manager_name: Optional[str] = None) -> Optional[Users]:
    """
    Знаходить об'єкт Users для менеджера, закріпленого за клієнтом.
    1. Якщо manager_name передано і не є заглушкою, використовуємо його.
    2. Якщо ні, шукаємо в довідниках: ClientManagerGuide, ClientAddress, DetailsForOrders.
    3. Шукаємо відповідного користувача в Users за full_name_for_orders (точний збіг, ilike, нормалізація).
    """
    target_manager = None
    if manager_name and manager_name.strip():
        m_stripped = manager_name.strip()
        if m_stripped.lower() not in ["", "невідомий", "менеджер", "null", "none", "undefined"]:
            target_manager = m_stripped

    if not target_manager and client_name and client_name.strip():
        clean_client = client_name.strip()
        try:
            guide_record = await ClientManagerGuide.select(ClientManagerGuide.manager).where(
                ClientManagerGuide.client == clean_client
            ).first().run()
            if guide_record and guide_record.get("manager"):
                target_manager = guide_record["manager"].strip()
        except Exception as e:
            logger.error(f"Помилка пошуку менеджера в ClientManagerGuide: {e}")

        if not target_manager:
            try:
                addr_record = await ClientAddress.select(ClientAddress.manager).where(
                    ClientAddress.client == clean_client
                ).first().run()
                if addr_record and addr_record.get("manager"):
                    target_manager = addr_record["manager"].strip()
            except Exception as e:
                logger.error(f"Помилка пошуку менеджера в ClientAddress: {e}")

        if not target_manager:
            try:
                order_record = await DetailsForOrders.select(DetailsForOrders.manager).where(
                    DetailsForOrders.client == clean_client
                ).first().run()
                if order_record and order_record.get("manager"):
                    target_manager = order_record["manager"].strip()
            except Exception as e:
                logger.error(f"Помилка пошуку менеджера в DetailsForOrders: {e}")

    if not target_manager:
        return None

    try:
        # 1. Точний збіг
        user = await Users.objects().where(Users.full_name_for_orders == target_manager).first().run()
        if user:
            return user

        # 2. Збіг без урахування регістру (ilike)
        user = await Users.objects().where(Users.full_name_for_orders.ilike(target_manager)).first().run()
        if user:
            return user

        # 3. Нормалізація пробілів (подвійні пробіли або пробіли по краях)
        norm_name = " ".join(target_manager.split())
        if norm_name != target_manager:
            user = await Users.objects().where(Users.full_name_for_orders.ilike(norm_name)).first().run()
            if user:
                return user
    except Exception as e:
        logger.error(f"Помилка пошуку менеджера в таблиці Users: {e}")

    return None


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

    # Отримуємо дані користувача з БД для перевірки ролі адміна/логіста
    user_in_db = await Users.objects().where(Users.telegram_id == telegram_id).first().run()
    is_admin_or_logist = (
        telegram_id in ALL_RECIPIENTS or 
        (user_in_db and getattr(user_in_db, "is_admin", False))
    )

    effective_created_by = telegram_id
    effective_manager_name = data.manager

    # Якщо заявку створює адмін або логіст — автором має стати менеджер клієнта
    if is_admin_or_logist:
        # Якщо вже передано явний override_created_by і він не є адміном/логістом (наприклад, при спліті)
        if data.override_created_by and data.override_created_by not in ALL_RECIPIENTS:
            effective_created_by = data.override_created_by
        else:
            manager_user = await resolve_client_manager_user(data.client, data.manager)
            if manager_user:
                effective_created_by = manager_user.telegram_id
                if manager_user.full_name_for_orders:
                    effective_manager_name = manager_user.full_name_for_orders
                logger.info(
                    f"👤 Автор доставки для клієнта '{data.client}' автоматично призначений на менеджера: "
                    f"{effective_manager_name} (TG ID: {effective_created_by}) замість ініціатора (TG ID: {telegram_id})"
                )
            else:
                logger.warning(
                    f"⚠️ Не знайдено Telegram-користувача для менеджера '{data.manager}' (клієнт: '{data.client}'). "
                    f"Автором залишається ініціатор (TG ID: {telegram_id})"
                )
    elif data.override_created_by:
        effective_created_by = data.override_created_by

    actor_display = data.actor_name or (
        (user_in_db.full_name_for_orders or f"{user_in_db.first_name} {user_in_db.last_name or ''}".strip())
        if user_in_db else "Логіст / Адміністратор"
    )

    # 1. Формування повідомлення для Telegram
    if data.status == "Самовивіз":
        header = "🚗 <b>Нова заявка на Самовивіз!</b>"
    elif data.status == "Нова Пошта":
        header = "📦 <b>Нова заявка на Нову Пошту!</b>"
    else:
        header = "🆕 <b>Нова заявка на доставку!</b>"

    message_lines = [
        header,
        "",
        f"👤 Менеджер: {effective_manager_name}",
        f"🚚 Контрагент: <code>{data.client}</code>",
    ]
    if telegram_id != effective_created_by:
        message_lines.append(f"✍️ Створив: {actor_display}")

    message_lines.extend([
        f"📍 Адреса: {data.address}",
        f"👤 Контакт: {data.contact}",
        f"📞 Телефон: {data.phone}",
        f"📅 Дата доставки: {data.date}",
        f"⚖️ Вага: {data.total_weight} кг",
        f"💬 Коментар: {data.comment}",
        "",
    ])

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
                event_creator=effective_created_by,
                event_creator_name=effective_manager_name,
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
            manager=effective_manager_name,
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
            created_by=effective_created_by,
            calendar_id=calendar_id,
        )
        await new_delivery.save().run()
        logger.info(f"✅ Основна інформація по доставці ID: {new_delivery.id} збережена (автор: {effective_created_by}).")

        items_to_insert = []
        for order in data.orders:
            for item in order.items:
                if (float(item.quantity or 0) <= 0):
                    continue
                active_parties = [p for p in item.parties if p.moved_q > 0] if item.parties else []
                if active_parties:
                    for party in active_parties:
                        items_to_insert.append(
                            DeliveryItems(
                                delivery=new_delivery.id,
                                order_ref=order.order,
                                product=item.product,
                                quantity=item.quantity,
                                party=party.party,
                                party_quantity=party.moved_q,
                                line_of_business=item.line_of_business,
                            )
                        )
                else:
                    items_to_insert.append(
                        DeliveryItems(
                            delivery=new_delivery.id,
                            order_ref=order.order,
                            product=item.product,
                            quantity=item.quantity,
                            line_of_business=item.line_of_business,
                        )
                    )
        if items_to_insert:
            await DeliveryItems.insert(*items_to_insert).run()
            logger.info(f"✅ {len(items_to_insert)} позицій по доставці збережено.")
            
        # 4. Відправка повідомлень администраторам та логістам
        # Використовуємо notify_new_delivery, щоб повідомлення було зареєстровано в БД та могло бути видалено пізніше
        if SEND_NOTIFICATIONS:
            await notify_new_delivery(delivery=new_delivery, custom_text=message)

    except Exception as e:
        logger.error(f"❌ Помилка збереження доставки в БД: {e}")
        raise HTTPException(status_code=500, detail=f"Помилка збереження в БД: {e}")

    # 5. Відправка повідомлень власнику та ініціатору
    owner_id = effective_created_by
    if owner_id not in ALL_RECIPIENTS:
        if SEND_NOTIFICATIONS:
            try:
                if telegram_id != owner_id:
                    owner_header = (
                        f"ℹ️ <b>Для вашого клієнта зареєстровано доставку!</b>\n"
                        f"<i>(Ініціатор: {html.escape(actor_display)})</i>\n"
                    )
                    await bot.send_message(chat_id=owner_id, text=owner_header, parse_mode='HTML')
                else:
                    await bot.send_message(chat_id=owner_id, text='<b>Ви успішно зареєстрували доставку:</b>', parse_mode='HTML')
                await bot.send_message(chat_id=owner_id, text=message, parse_mode='HTML')
            except Exception as e:
                logger.error(f'Помилка при сповіщенні власника {owner_id}: {e}')
    
    if telegram_id not in ALL_RECIPIENTS and telegram_id != owner_id:
        if SEND_NOTIFICATIONS:
            try:
                await bot.send_message(
                    chat_id=telegram_id, 
                    text=f'✅ Ви успішно зареєстрували доставку для менеджера {effective_manager_name}. Дякуємо за роботу!'
                )
            except Exception as e:
                logger.error(f'Помилка при сповіщенні ініціатора {telegram_id}: {e}')

    
    # 5. Уведомление через WebSocket
    await manager.broadcast({
        "type": "DELIVERY_CREATED",
        "payload": {"id": new_delivery.id, "client": data.client}
    })

    return {"status": "ok", "id": new_delivery.id}


@app.post("/delivery/split", dependencies=[Depends(check_not_guest)])
async def split_delivery(
    data: SplitDeliveryRequest,
    background_tasks: BackgroundTasks,
    X_Telegram_Init_Data: str = Header(),
):
    """
    Атомарне розділення доставки.
    Фронтенд надсилає ID доставки та список товарів із кількостями для перенесення.
    Бекенд в одній транзакції:
      1. Створює нову доставку (клон метаданих з приміткою "(Розділено)")
      2. Переносить товари з пропорційним розподілом партій
      3. Оновлює або видаляє оригінальну доставку
    """
    parsed_init_data = check_telegram_auth(X_Telegram_Init_Data)
    if not parsed_init_data:
        raise HTTPException(status_code=401, detail="Unauthorized")

    user_info_str = parsed_init_data.get("user")
    user_data = json.loads(user_info_str)
    telegram_id = user_data.get("id")

    # Завантаження оригінальної доставки
    original = await Deliveries.objects().where(Deliveries.id == data.delivery_id).first().run()
    if not original:
        raise HTTPException(status_code=404, detail="Доставку не знайдено")

    # Завантаження всіх позицій доставки
    db_items = await DeliveryItems.select().where(
        DeliveryItems.delivery == data.delivery_id
    ).run()
    if not db_items:
        raise HTTPException(status_code=400, detail="Доставка не містить товарів")

    # Групування позицій по (product, order_ref)
    groups: Dict[tuple, list] = {}
    for row in db_items:
        key = (row["product"], row.get("order_ref") or "")
        groups.setdefault(key, []).append(row)

    # Валідація та побудова списку перенесених / залишених позицій
    new_items_to_insert = []    # Для нової доставки
    remain_items_to_insert = [] # Для оригіналу (залишок)
    matched_keys = set()

    for split_item in data.items:
        transfer_qty = split_item.transfer_quantity
        if transfer_qty <= 0:
            continue

        # Пошук відповідної групи
        key = (split_item.product, split_item.order_ref or "")
        group = groups.get(key)
        if not group:
            # Спроба знайти тільки за product (без order_ref)
            key = next(
                (k for k in groups if k[0] == split_item.product and k not in matched_keys),
                None,
            )
            if key:
                group = groups[key]
            else:
                logger.warning(
                    f"⚠️ Split: товар '{split_item.product}' (order_ref='{split_item.order_ref}') "
                    f"не знайдено в доставці {data.delivery_id}. Пропущено."
                )
                continue

        matched_keys.add(key)

        # Загальна кількість в групі (quantity однакове для всіх партій одного товару)
        group_total_qty = group[0]["quantity"]
        if group_total_qty <= 0:
            continue

        # Обмеження: не можна перенести більше, ніж є
        transfer_qty = min(transfer_qty, group_total_qty)
        remain_qty = round(group_total_qty - transfer_qty, 3)

        # Пропорція для розподілу партій
        total_party_sum = sum(float(row.get("party_quantity") or 0) for row in group)
        transfer_ratio = transfer_qty / total_party_sum if total_party_sum > 0 else 1.0

        order_ref = group[0].get("order_ref") or ""
        line_of_business = group[0].get("line_of_business")

        for row in group:
            party_name = row.get("party") or ""
            party_qty = float(row.get("party_quantity") or 0)
            warehouse = row.get("warehouse")

            if party_qty <= 0 and not party_name:
                # Товар без партій — просто за кількістю
                new_items_to_insert.append(DeliveryItems(
                    delivery=0,  # placeholder, буде замінено в транзакції
                    order_ref=order_ref,
                    product=split_item.product,
                    quantity=transfer_qty,
                    line_of_business=line_of_business,
                ))
                if remain_qty > 0.001:
                    remain_items_to_insert.append(DeliveryItems(
                        delivery=data.delivery_id,
                        order_ref=order_ref,
                        product=split_item.product,
                        quantity=remain_qty,
                        line_of_business=line_of_business,
                    ))
                continue

            # Пропорційний розподіл партії
            transferred_party_qty = round(party_qty * transfer_ratio, 3)
            remaining_party_qty = round(party_qty - transferred_party_qty, 3)

            if transferred_party_qty > 0.001:
                new_items_to_insert.append(DeliveryItems(
                    delivery=0,  # placeholder
                    order_ref=order_ref,
                    product=split_item.product,
                    quantity=transfer_qty,
                    party=party_name,
                    party_quantity=transferred_party_qty,
                    warehouse=warehouse,
                    line_of_business=line_of_business,
                ))

            if remaining_party_qty > 0.001:
                remain_items_to_insert.append(DeliveryItems(
                    delivery=data.delivery_id,
                    order_ref=order_ref,
                    product=split_item.product,
                    quantity=remain_qty,
                    party=party_name,
                    party_quantity=remaining_party_qty,
                    warehouse=warehouse,
                    line_of_business=line_of_business,
                ))

    # Додаємо до залишку ті позиції, які НЕ були обрані для розділення
    for key, group in groups.items():
        if key in matched_keys:
            continue
        for row in group:
            remain_items_to_insert.append(DeliveryItems(
                delivery=data.delivery_id,
                order_ref=row.get("order_ref") or "",
                product=row["product"],
                quantity=row["quantity"],
                party=row.get("party"),
                party_quantity=row.get("party_quantity"),
                warehouse=row.get("warehouse"),
                line_of_business=row.get("line_of_business"),
            ))

    if not new_items_to_insert:
        raise HTTPException(status_code=400, detail="Немає товарів для перенесення")

    # Пропорційний розрахунок ваги
    original_total_qty = sum(float(row["quantity"]) for row in db_items)
    transferred_total_qty = sum(float(si.transfer_quantity) for si in data.items if si.transfer_quantity > 0)
    if original_total_qty > 0:
        weight_ratio = min(transferred_total_qty / original_total_qty, 1.0)
    else:
        weight_ratio = 0.5
    new_total_weight = round((original.total_weight or 0) * weight_ratio, 2)
    remain_total_weight = round((original.total_weight or 0) - new_total_weight, 2)

    # Коментар для нової доставки
    base_comment = original.comment or ""
    new_comment = f"{base_comment} (Розділено)".strip()

    # === ТРАНЗАКЦІЯ ===
    new_delivery_id = None
    original_deleted = False

    try:
        async with Deliveries._meta.db.transaction():
            # 1. Створення нової доставки
            new_delivery = Deliveries(
                client=original.client,
                manager=original.manager,
                address=original.address,
                contact=original.contact,
                phone=original.phone,
                delivery_date=original.delivery_date,
                comment=new_comment,
                is_custom_address=original.is_custom_address,
                latitude=original.latitude,
                longitude=original.longitude,
                total_weight=new_total_weight,
                status=original.status or "Створено",
                created_by=original.created_by,
            )
            await new_delivery.save().run()
            new_delivery_id = new_delivery.id

            # 2. Вставка перенесених товарів (з правильним delivery ID)
            for item in new_items_to_insert:
                item.delivery = new_delivery_id
            await DeliveryItems.insert(*new_items_to_insert).run()

            # 3. Видалення старих позицій оригіналу
            await DeliveryItems.delete().where(
                DeliveryItems.delivery == data.delivery_id
            ).run()

            # 4. Вставка залишку або видалення оригіналу
            if remain_items_to_insert:
                await DeliveryItems.insert(*remain_items_to_insert).run()
                # Оновлення ваги оригіналу
                original.total_weight = remain_total_weight
                await original.save().run()
            else:
                # Всі товари перенесені — видаляємо оригінал
                await Deliveries.delete().where(
                    Deliveries.id == data.delivery_id
                ).run()
                original_deleted = True

        logger.info(
            f"✅ Доставку {data.delivery_id} розділено. "
            f"Нова доставка ID: {new_delivery_id}. "
            f"Оригінал {'видалено' if original_deleted else 'оновлено'}."
        )

    except Exception as e:
        logger.error(f"❌ Помилка транзакції розділення доставки {data.delivery_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Помилка розділення доставки: {e}",
        )

    # === ПІСЛЯ ТРАНЗАКЦІЇ: Сповіщення (не блокують результат) ===
    warnings = []

    # Календар для нової доставки
    try:
        from .models import DeliveryRequest as DR
        cal_data = type("CalData", (), {
            "client": original.client,
            "date": str(original.delivery_date or ""),
            "address": original.address or "",
            "comment": new_comment,
        })()
        calendar = await create_calendar_event(cal_data)
        if calendar:
            calendar_id = calendar.get("id")
            await Deliveries.update({Deliveries.calendar_id: calendar_id}).where(
                Deliveries.id == new_delivery_id
            ).run()
            start_info = calendar.get("start", {})
            date_str = start_info.get("date") or start_info.get("dateTime")
            if date_str:
                date_val = datetime.fromisoformat(date_str).date()
                await Events.insert(Events(
                    event_id=calendar_id,
                    event_creator=original.created_by or telegram_id,
                    event_creator_name=original.manager or "",
                    event_status=0,
                    start_event=date_val,
                    event=original.client,
                )).run()
    except Exception as e:
        logger.warning(f"⚠️ Календар для розділеної доставки: {e}")
        warnings.append(f"Помилка календаря: {e}")

    # Telegram-сповіщення
    if SEND_NOTIFICATIONS:
        try:
            actor_display = data.actor_name or "Логіст"
            # Будуємо повідомлення
            items_lines = []
            for si in data.items:
                if si.transfer_quantity > 0:
                    items_lines.append(f"  🔹 {si.product}: <b>{si.transfer_quantity}</b>")
            items_text = "\n".join(items_lines) if items_lines else "<i>(не вказано)</i>"

            msg = (
                f"✂️ <b>Доставку розділено</b>\n\n"
                f"👤 Клієнт: <b>{html.escape(original.client)}</b>\n"
                f"📦 Перенесено в нову доставку (ID {new_delivery_id}):\n{items_text}\n\n"
                f"✍️ Ініціатор: {html.escape(actor_display)}\n"
            )
            if original_deleted:
                msg += "🗑 Оригінальну доставку видалено (всі товари перенесені)\n"
            else:
                msg += f"📋 Оригінал (ID {data.delivery_id}) оновлено\n"

            recipients = set(ALL_RECIPIENTS)
            if original.created_by:
                recipients.add(original.created_by)

            for rid in recipients:
                try:
                    await bot.send_message(chat_id=rid, text=msg, parse_mode="HTML")
                except Exception as tg_err:
                    logger.warning(f"⚠️ Telegram notify {rid}: {tg_err}")
        except Exception as e:
            logger.warning(f"⚠️ Telegram сповіщення розділення: {e}")
            warnings.append(f"Помилка Telegram: {e}")

    # WebSocket
    await manager.broadcast({
        "type": "DELIVERY_SPLIT",
        "payload": {
            "original_id": data.delivery_id,
            "new_id": new_delivery_id,
            "original_deleted": original_deleted,
        },
    })

    return {
        "status": "ok",
        "new_delivery_id": new_delivery_id,
        "original_deleted": original_deleted,
        "warnings": warnings,
    }


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

        # Оновлюємо атрибути, якщо вони передані
        if data.ttn is not None:
            delivery_data.ttn = data.ttn
        if data.total_weight is not None:
            delivery_data.total_weight = data.total_weight
        if data.address is not None:
            delivery_data.address = data.address
        if data.contact is not None:
            delivery_data.contact = data.contact
        if data.phone is not None:
            delivery_data.phone = data.phone
        if data.comment is not None:
            delivery_data.comment = data.comment
        if data.latitude is not None:
            delivery_data.latitude = data.latitude
        if data.longitude is not None:
            delivery_data.longitude = data.longitude

        # 2. Оновлюємо статус, якщо змінився
        status_changed = False
        old_status = delivery_data.status
        if delivery_data.status != data.status:
            status_changed = True
            delivery_data.status = data.status
            
            # Повідомлення про зміну статусу
            try:
                # Витягуємо ID користувача з JSON-рядка 'user'
                user_data_json = parsed_init_data.get("user")
                user_id = None
                if user_data_json:
                    try:
                        user_id = json.loads(user_data_json).get("id")
                    except Exception:
                        pass

                if old_status == "Потрібні дані НП" and data.status == "Нова Пошта":
                    await notify_np_details_filled(
                        delivery=delivery_data,
                        actor_name=data.actor_name
                    )
                else:
                    await notify_delivery_status_change(
                        delivery=delivery_data, 
                        status=data.status, 
                        actor_name=data.actor_name,
                        actor_id=user_id
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

            # Додаткові сповіщення менеджеру та логістам/адмінам при певних статусах
            if data.status == 'Виконано':
                try:
                    from .services.delivery_reminder_service import cancel_np_reminder
                    await cancel_np_reminder(delivery_data.id)
                except Exception as rem_err:
                    logger.warning(f"Error cancelling np reminder: {rem_err}")

                try:
                    items_list = [
                        f"🔹 {item.product}: <b>{item.quantity}</b>"
                        for item in data.items
                        if item.product and (float(item.quantity or 0) > 0)
                    ]
                    items_text = "\n".join(items_list) if items_list else "<i>(не вказано)</i>"
                    message_text = (
                        f"✅ <b>Доставка завершена</b>\n\n"
                        f"👤 Клієнт: <b>{delivery_data.client}</b>\n"
                        f"📦 Товари:\n{items_text}\n"
                    )
                    if delivery_data.ttn:
                        message_text += f"\n📦 <b>ТТН:</b> <code>{delivery_data.ttn}</code>\n"
                        # Запитуємо статус посилки з API Нової Пошти
                        try:
                            np_result = await call_np_api("TrackingDocument", "getStatusDocuments", {
                                "Documents": [{"DocumentNumber": delivery_data.ttn, "Phone": ""}]
                            })
                            if np_result.get("success") and np_result.get("data"):
                                track = np_result["data"][0]
                                status_desc = track.get("Status", "")
                                warehouse = track.get("WarehouseRecipient", "")
                                schedule = track.get("ScheduledDeliveryDate", "")
                                if status_desc:
                                    message_text += f"📍 <b>Статус:</b> {status_desc}\n"
                                if warehouse:
                                    message_text += f"🏢 <b>Відділення:</b> {warehouse}\n"
                                if schedule:
                                    message_text += f"📅 <b>Очікувана дата:</b> {schedule}\n"
                        except Exception as np_err:
                            logger.warning(f"Could not fetch NP tracking status: {np_err}")
                        message_text += f"\n🔗 <a href=\"https://novaposhta.ua/tracking/{delivery_data.ttn}\">Відстежити на сайті</a>"
                    
                    recipients = set(ALL_RECIPIENTS)
                    if delivery_data.created_by:
                        recipients.add(delivery_data.created_by)
                        
                    for recipient_id in recipients:
                        try:
                            await bot.send_message(
                                chat_id=recipient_id,
                                text=message_text,
                                parse_mode="HTML",
                                disable_web_page_preview=True
                            )
                        except Exception as tg_err:
                            logger.warning(f"Error sending completion message to {recipient_id}: {tg_err}")
                            
                except Exception as tg_err:
                    logger.warning(f"Error building completion message: {tg_err}")

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
                    items_list = [
                        f"🔹 {item.product}: <b>{item.quantity}</b>"
                        for item in data.items
                        if item.product and (float(item.quantity or 0) > 0)
                    ]
                    items_text = "\n".join(items_list) if items_list else "<i>(не вказано)</i>"
                    try:
                        await bot.send_message(
                            chat_id=delivery_data.created_by,
                            text=(
                                f"📦 <b>Продукція готова до відвантаження</b>\n\n"
                                f"👤 Клієнт: <b>{delivery_data.client}</b>\n"
                                f"📦 Товари:\n{items_text}\n\n"
                                f"<i>Підтвердіть дату та час з логістом.</i>\n"),
                            parse_mode="HTML",
                        )
                    except Exception as tg_err:
                        logger.warning(f"Error sending ready message: {tg_err}")

        # 3. Зберігаємо зміни доставки
        await delivery_data.save().run()

        # 4. Оновлюємо склад доставки (позиції та партії)
        async with DeliveryItems._meta.db.transaction():
            await DeliveryItems.delete().where(
                DeliveryItems.delivery == data.delivery_id
            ).run()

            items_to_insert = []
            for item in data.items:
                if (float(item.quantity or 0) <= 0):
                    continue
                active_parties = [p for p in item.parties if p.moved_q > 0] if item.parties else []
                if active_parties:
                    for party in active_parties:
                        items_to_insert.append(
                            DeliveryItems(
                                delivery=data.delivery_id,
                                order_ref=item.order_ref,
                                product=item.product,
                                quantity=item.quantity,
                                party=party.party,
                                party_quantity=party.moved_q,
                                warehouse=party.warehouse,
                                line_of_business=item.line_of_business,
                            )
                        )
                else:
                    items_to_insert.append(
                        DeliveryItems(
                            delivery=data.delivery_id,
                            order_ref=item.order_ref,
                            product=item.product,
                            quantity=item.quantity,
                            line_of_business=item.line_of_business,
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

        # Надіслати сповіщення менеджеру про взяття в роботу (робиться в кінці, коли DeliveryItems вже збережені)
        if data.status == 'В роботі':
            # Планування нагадування для Нової Пошти через 15 хвилин
            if status_changed:
                try:
                    from .services.delivery_reminder_service import is_np_delivery, schedule_np_reminder
                    if is_np_delivery(delivery_data, old_status):
                        user_data_json = parsed_init_data.get("user")
                        taker_id = None
                        if user_data_json:
                            try:
                                taker_id = json.loads(user_data_json).get("id")
                            except Exception:
                                pass
                        if taker_id:
                            await schedule_np_reminder(delivery_data.id, taker_id, delay_minutes=15)
                except Exception as rem_err:
                    logger.warning(f"Error scheduling np reminder: {rem_err}")

            if delivery_data.created_by:
                try:
                    from .utils import format_delivery_final_data
                    final_data_text = await format_delivery_final_data(delivery_data.id)
                    await bot.send_message(
                        chat_id=delivery_data.created_by,
                        text=(
                            f"🚚 <b>Доставка взята в роботу</b>\n\n"
                            f"👤 Клієнт: <b>{delivery_data.client}</b>\n\n"
                            f"{final_data_text}"
                        ),
                        parse_mode="HTML",
                    )
                except Exception as tg_err:
                    logger.warning(f"Error sending in-progress message to manager: {tg_err}")

        # 5. Уведомление через WebSocket
        await manager.broadcast({
            "type": "DELIVERY_UPDATED",
            "payload": {"id": data.delivery_id, "status": data.status}
        })

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


@app.get("/accountants", dependencies=[Depends(check_not_guest)])
async def get_accountants(X_Telegram_Init_Data: str = Header()):
    """Отримати список активних бухгалтерів для вибору в інтерфейсі"""
    accountants = await Accountants.select(
        Accountants.id,
        Accountants.name,
        Accountants.email,
        Accountants.telegram_id,
        Accountants.telegram_username,
        Accountants.phone,
        Accountants.is_default
    ).where(Accountants.is_active == True).run()
    
    serialized = []
    for acc in accountants:
        serialized.append({
            "id": str(acc["id"]),
            "name": acc["name"],
            "email": acc["email"] or "",
            "telegram_id": acc["telegram_id"],
            "telegram_username": acc["telegram_username"] or "",
            "phone": acc["phone"] or "",
            "is_default": bool(acc["is_default"])
        })

    return {"status": "ok", "accountants": serialized}


@app.get("/accountants/for-manager", dependencies=[Depends(check_not_guest)])
async def get_accountant_for_manager(manager: str = Query(...), X_Telegram_Init_Data: str = Header()):
    """Отримати закріпленого бухгалтера для менеджера або дефолтного"""
    clean_mgr = manager.strip()
    user = None
    if clean_mgr.isdigit():
        user = await Users.objects().where(Users.telegram_id == int(clean_mgr)).first().run()
    if not user:
        user = await Users.objects().where(
            (Users.full_name_for_orders.ilike(clean_mgr)) |
            (Users.username.ilike(clean_mgr.lstrip('@')))
        ).first().run()

    link = None
    if user:
        link = await ManagerAccountantGuide.objects().where(
            ManagerAccountantGuide.manager == user.telegram_id
        ).first().run()

    accountant = None
    if link and link.accountant:
        acc_obj = await Accountants.objects().where(
            Accountants.id == link.accountant,
            Accountants.is_active == True
        ).first().run()
        if acc_obj:
            accountant = {
                "id": str(acc_obj.id),
                "name": acc_obj.name,
                "email": acc_obj.email or "",
                "telegram_id": acc_obj.telegram_id,
                "telegram_username": acc_obj.telegram_username or "",
                "phone": acc_obj.phone or "",
                "is_default": bool(acc_obj.is_default)
            }

    if not accountant:
        default_acc = await Accountants.objects().where(
            Accountants.is_default == True,
            Accountants.is_active == True
        ).first().run()
        if not default_acc:
            default_acc = await Accountants.objects().where(
                Accountants.is_active == True
            ).first().run()
        if default_acc:
            accountant = {
                "id": str(default_acc.id),
                "name": default_acc.name,
                "email": default_acc.email or "",
                "telegram_id": default_acc.telegram_id,
                "telegram_username": default_acc.telegram_username or "",
                "phone": default_acc.phone or "",
                "is_default": bool(default_acc.is_default)
            }

    return {"status": "ok", "accountant": accountant}


@app.post("/delivery/send-to-accountant", dependencies=[Depends(check_not_guest)])
async def send_delivery_to_accountant(
    data: SendToAccountantRequest,
    X_Telegram_Init_Data: str = Header()
):
    """
    Зберігає актуальні партії (якщо передані) та надсилає дані доставки бухгалтеру
    через обрані канали (Telegram, Email) + повертає посилання mailto для відкриття поштової програми.
    """
    delivery = await Deliveries.objects().where(Deliveries.id == data.delivery_id).first().run()
    if not delivery:
        raise HTTPException(status_code=404, detail="Доставку не знайдено")

    if data.ttn and data.ttn != delivery.ttn:
        delivery.ttn = data.ttn
        await delivery.save().run()

    # Збереження оновлених партій та товарів (якщо передані)
    if data.orders is not None:
        async with DeliveryItems._meta.db.transaction():
            await DeliveryItems.delete().where(DeliveryItems.delivery == data.delivery_id).run()
            items_to_insert = []
            for order in data.orders:
                for item in order.items:
                    if float(item.quantity or 0) <= 0:
                        continue
                    active_parties = [p for p in item.parties if p.moved_q > 0] if item.parties else []
                    if active_parties:
                        for party in active_parties:
                            items_to_insert.append(
                                DeliveryItems(
                                    delivery=data.delivery_id,
                                    order_ref=order.order_ref,
                                    product=item.product,
                                    quantity=item.quantity,
                                    party=party.party,
                                    party_quantity=party.moved_q,
                                    warehouse=party.warehouse,
                                    line_of_business=item.line_of_business,
                                )
                            )
                    else:
                        items_to_insert.append(
                            DeliveryItems(
                                delivery=data.delivery_id,
                                order_ref=order.order_ref,
                                product=item.product,
                                quantity=item.quantity,
                                line_of_business=item.line_of_business,
                            )
                        )
            if items_to_insert:
                await DeliveryItems.insert(*items_to_insert).run()
    elif data.items is not None:
        async with DeliveryItems._meta.db.transaction():
            await DeliveryItems.delete().where(DeliveryItems.delivery == data.delivery_id).run()
            items_to_insert = []
            for item in data.items:
                if (float(item.quantity or 0) <= 0):
                    continue
                active_parties = [p for p in item.parties if p.moved_q > 0] if item.parties else []
                if active_parties:
                    for party in active_parties:
                        items_to_insert.append(
                            DeliveryItems(
                                delivery=data.delivery_id,
                                order_ref=item.order_ref,
                                product=item.product,
                                quantity=item.quantity,
                                party=party.party,
                                party_quantity=party.moved_q,
                                warehouse=party.warehouse,
                                line_of_business=item.line_of_business,
                            )
                        )
                else:
                    items_to_insert.append(
                        DeliveryItems(
                            delivery=data.delivery_id,
                            order_ref=item.order_ref,
                            product=item.product,
                            quantity=item.quantity,
                            line_of_business=item.line_of_business,
                        )
                    )
            if items_to_insert:
                await DeliveryItems.insert(*items_to_insert).run()

    # Підготовка даних замовлень для звіту бухгалтеру
    orders_for_report = []
    if data.orders is not None and len(data.orders) > 0:
        for o in data.orders:
            orders_for_report.append({
                "order_ref": o.order_ref,
                "client": o.client,
                "manager": o.manager,
                "address": o.address,
                "comment": o.comment or "",
                "items": [
                    {
                        "product": it.product,
                        "nomenclature": it.nomenclature or it.product,
                        "quantity": it.quantity,
                        "weight": it.weight or 0.0,
                        "line_of_business": it.line_of_business,
                        "parties": [
                            {
                                "party": p.party,
                                "moved_q": p.moved_q,
                                "party_quantity": p.moved_q,
                                "warehouse": p.warehouse or "",
                            }
                            for p in it.parties if p.moved_q > 0
                        ]
                    }
                    for it in o.items if (float(it.quantity or 0) > 0)
                ]
            })
    else:
        # Fallback з бази DeliveryItems
        db_items = await DeliveryItems.objects().where(DeliveryItems.delivery == data.delivery_id).run()
        grouped_orders_dict = {}
        for it in db_items:
            o_ref = it.order_ref or "Без доповнення"
            if o_ref not in grouped_orders_dict:
                grouped_orders_dict[o_ref] = {
                    "order_ref": o_ref,
                    "client": delivery.client or "Не вказано",
                    "manager": delivery.manager or "",
                    "address": delivery.address or "",
                    "comment": delivery.comment or "",
                    "items": {}
                }
            p_key = it.product
            if p_key not in grouped_orders_dict[o_ref]["items"]:
                grouped_orders_dict[o_ref]["items"][p_key] = {
                    "product": it.product,
                    "nomenclature": it.product,
                    "quantity": it.quantity,
                    "line_of_business": it.line_of_business,
                    "parties": []
                }
            if it.party:
                grouped_orders_dict[o_ref]["items"][p_key]["parties"].append({
                    "party": it.party,
                    "party_quantity": it.party_quantity or it.quantity,
                    "moved_q": it.party_quantity or it.quantity,
                    "warehouse": it.warehouse or "",
                })

        for o_ref, o_data in grouped_orders_dict.items():
            orders_for_report.append({
                "order_ref": o_ref,
                "client": o_data["client"],
                "manager": o_data["manager"],
                "address": o_data["address"],
                "comment": o_data.get("comment") or "",
                "items": list(o_data["items"].values())
            })

    # Пошук закріпленого бухгалтера
    accountant = None
    if data.accountant_id:
        accountant = await Accountants.objects().where(
            Accountants.id == data.accountant_id,
            Accountants.is_active == True
        ).first().run()

    # Якщо бухгалтер не обраний вручну, спробуємо знайти за менеджером замовлення
    lead_manager = None
    for o in orders_for_report:
        if o.get("manager"):
            lead_manager = o["manager"].strip()
            break
    if not lead_manager and delivery.manager:
        lead_manager = delivery.manager.strip()

    if not accountant and lead_manager:
        user = None
        if lead_manager.isdigit():
            user = await Users.objects().where(Users.telegram_id == int(lead_manager)).first().run()
        if not user:
            user = await Users.objects().where(
                (Users.full_name_for_orders.ilike(lead_manager)) |
                (Users.username.ilike(lead_manager.lstrip('@')))
            ).first().run()

        if user:
            link = await ManagerAccountantGuide.objects().where(
                ManagerAccountantGuide.manager == user.telegram_id
            ).first().run()
            if link and link.accountant:
                accountant = await Accountants.objects().where(
                    Accountants.id == link.accountant,
                    Accountants.is_active == True
                ).first().run()

    if not accountant:
        accountant = await Accountants.objects().where(
            Accountants.is_default == True,
            Accountants.is_active == True
        ).first().run()

    if not accountant:
        accountant = await Accountants.objects().where(
            Accountants.is_active == True
        ).first().run()

    if not accountant:
        raise HTTPException(
            status_code=400,
            detail="Не знайдено жодного активного бухгалтера в системі. Будь ласка, додайте бухгалтера через панель керування."
        )

    # Збираємо унікальних клієнтів, менеджерів та доповнення
    unique_clients = list(dict.fromkeys([o["client"] for o in orders_for_report if o.get("client") and o["client"] != "Не вказано"]))
    unique_managers = list(dict.fromkeys([o["manager"] for o in orders_for_report if o.get("manager")]))
    unique_order_refs = list(dict.fromkeys([o["order_ref"] for o in orders_for_report if o.get("order_ref") and o["order_ref"] != "—"]))

    delivery_dict = {
        "id": delivery.id,
        "client": ", ".join(unique_clients) if unique_clients else (delivery.client or ""),
        "clients": unique_clients,
        "manager": ", ".join(unique_managers) if unique_managers else (delivery.manager or ""),
        "managers": unique_managers,
        "order_refs": unique_order_refs,
        "ttn": delivery.ttn,
        "delivery_date": str(delivery.delivery_date) if delivery.delivery_date else "",
        "address": delivery.address,
        "contact": delivery.contact,
        "phone": delivery.phone,
        "comment": delivery.comment
    }

    printable_html = generate_printable_html(delivery_dict, orders=orders_for_report, custom_comment=data.comment)
    mailto_url = generate_mailto_url(accountant.email or "", delivery_dict, orders=orders_for_report, custom_comment=data.comment)

    res_tg = {"success": False, "skipped": True}
    res_mail = {"success": False, "skipped": True}
    warnings = []

    if "telegram" in data.channels:
        if accountant.telegram_id:
            res_tg = await send_accountant_telegram(
                telegram_id=accountant.telegram_id,
                delivery_data=delivery_dict,
                orders=orders_for_report,
                printable_html=printable_html,
                custom_comment=data.comment
            )
            if not res_tg.get("success"):
                warnings.append(f"Telegram: {res_tg.get('error')}")
        else:
            res_tg = {"success": False, "error": "Telegram ID не вказано для цього бухгалтера"}
            warnings.append("Telegram ID не вказано для обраного бухгалтера")

    if "email" in data.channels:
        if accountant.email:
            res_mail = await send_accountant_email(
                to_email=accountant.email,
                delivery_data=delivery_dict,
                orders=orders_for_report,
                printable_html=printable_html,
                custom_comment=data.comment
            )
            if not res_mail.get("success") and not res_mail.get("skipped"):
                warnings.append(f"Email: {res_mail.get('error')}")
        else:
            res_mail = {"success": False, "error": "Email не вказано для цього бухгалтера"}
            warnings.append("Email не вказано для обраного бухгалтера")

    return {
        "status": "ok",
        "accountant": {
            "id": str(accountant.id),
            "name": accountant.name,
            "email": accountant.email,
            "telegram_id": accountant.telegram_id,
            "telegram_username": accountant.telegram_username
        },
        "telegram": res_tg,
        "email": res_mail,
        "mailto_url": mailto_url,
        "warnings": warnings,
        "printable_html": printable_html
    }


async def update_delivery_date(
    data: ChangeDeliveryDateRequest,
    user: dict = Depends(get_current_telegram_user)
):

    """
    Оновлює дату доставки, оновлює подію в Google Calendar та відправляє повідомлення менеджеру.
    """
    user_id = user["telegram_id"]
    actor_name = user["full_name_for_orders"] or user["first_name"]

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

        # 4. Сповіщення
        if SEND_NOTIFICATIONS:
            # Сповіщення логістам (видаляє старі повідомлення)
            await notify_delivery_date_change(delivery, new_date_obj, actor_name, user_id)

            # Сповіщення менеджеру
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

        # 5. Уведомление через WebSocket
        await manager.broadcast({
            "type": "DELIVERY_UPDATED",
            "payload": {"id": data.delivery_id, "delivery_date": str(new_date_obj)}
        })

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
    user: dict = Depends(get_current_telegram_user)
):
    """
    Масове оновлення статусу або дати для списку доставок.
    Групує сповіщення по менеджерах та надсилає індивідуальні сповіщення логістам.
    """
    user_id = user["telegram_id"]
    actor_name = user["full_name_for_orders"] or user["first_name"]

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
                
                # Оновлення ТТН якщо передано
                deliv_ttn = None
                if data.ttn_map:
                    deliv_ttn = data.ttn_map.get(str(delivery.id)) or data.ttn_map.get(delivery.id)
                elif data.common_ttn:
                    deliv_ttn = data.common_ttn

                if deliv_ttn is not None:
                    cleaned_ttn = str(deliv_ttn).strip()
                    if cleaned_ttn and delivery.ttn != cleaned_ttn:
                        delivery.ttn = cleaned_ttn
                        changes.append(f"ТТН: <code>{cleaned_ttn}</code>")

                # Оновлення статусу
                if data.status and delivery.status != data.status:
                    old_status = delivery.status
                    delivery.status = data.status
                    changes.append(f"статус: {old_status} ➔ <b>{data.status}</b>")
                    
                    # Нагадування для Нової Пошти
                    if data.status == "В роботі":
                        try:
                            from .services.delivery_reminder_service import is_np_delivery, schedule_np_reminder
                            if is_np_delivery(delivery, old_status):
                                await schedule_np_reminder(delivery.id, user_id, delay_minutes=15)
                        except Exception as rem_err:
                            logger.warning(f"Error scheduling np reminder in batch: {rem_err}")
                    elif data.status == "Виконано":
                        try:
                            from .services.delivery_reminder_service import cancel_np_reminder
                            await cancel_np_reminder(delivery.id)
                        except Exception as rem_err:
                            logger.warning(f"Error cancelling np reminder in batch: {rem_err}")

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
                    
                    # Сповіщення логістам (видаляє старі повідомлення)
                    if data.status and data.new_date:
                        # Якщо змінено і те, і інше, надсилаємо про статус (там є дата)
                        await notify_delivery_status_change(delivery, data.status, actor_name, user_id)
                    elif data.status:
                        await notify_delivery_status_change(delivery, data.status, actor_name, user_id)
                    elif data.new_date:
                        await notify_delivery_date_change(delivery, data.new_date, actor_name, user_id)

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

        # 4. Уведомление через WebSocket
        await manager.broadcast({
            "type": "DELIVERIES_BATCH_UPDATED",
            "payload": {
                "ids": data.delivery_ids,
                "status": data.status,
                "new_date": data.new_date,
                "ttns": {d.id: d.ttn for d in deliveries_to_update if d.ttn}
            }
        })

        logger.info(f"✅ Успішно оновлено {len(deliveries_to_update)} доставок пакетно.")
        return {"status": "ok", "message": f"Successfully updated {len(deliveries_to_update)} deliveries."}

    except Exception as e:
        logger.info(f"❌ Помилка пакетного оновлення доставок: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Не вдалося оновити доставки пакетно: {e}",
        )


@app.post("/delivery/change_date", dependencies=[Depends(check_not_guest)])
async def change_delivery_date(
    data: ChangeDeliveryDateRequest,
    user: dict = Depends(get_current_telegram_user)
):
    """
    Змінює дату доставки та оновлює її в Google Календарі та таблиці Events.
    """
    user_id = user["telegram_id"]
    actor_name = user["full_name_for_orders"] or user["first_name"]
    try:

        # 1. Отримуємо доставку
        delivery = await Deliveries.objects().where(Deliveries.id == data.delivery_id).first().run()
        if not delivery:
            raise HTTPException(status_code=404, detail="Доставку не знайдено")

        new_date_obj = datetime.strptime(data.new_date, "%Y-%m-%d").date()
        old_date = delivery.delivery_date
        
        # 2. Оновлюємо в базі Deliveries
        delivery.delivery_date = new_date_obj
        await delivery.save().run()

        # 3. Оновлюємо в Google Календарі та таблиці Events
        if delivery.calendar_id:
            try:
                # Оновлюємо Google Calendar
                changed_date_calendar_events_by_id(event_id=delivery.calendar_id, new_delivery_date=new_date_obj)
                
                # Оновлюємо таблицю Events (для звітів)
                await Events.update({Events.start_event: new_date_obj}).where(
                    Events.event_id == delivery.calendar_id
                ).run()
                
                logger.info(f"📅 Дата доставки {delivery.id} змінена з {old_date} на {new_date_obj} (Календар оновлено)")
            except Exception as cal_err:
                logger.error(f"Error updating calendar date: {cal_err}")

        # 4. Сповіщення менеджеру
        message_text = (
            f"📅 <b>Змінено дату доставки</b>\n\n"
            f"👤 Клієнт: <b>{delivery.client}</b>\n"
            f"🔄 Дата: {old_date} ➔ <b>{new_date_obj}</b>"
        )
        
        if SEND_NOTIFICATIONS:
            # Повідомляємо логістів (видаляє старі повідомлення)
            await notify_delivery_date_change(delivery, new_date_obj, actor_name, user_id)

            # Повідомляємо менеджера
            if delivery.created_by:
                try:
                    await bot.send_message(chat_id=delivery.created_by, text=message_text, parse_mode="HTML")
                except Exception as tg_err:
                    logger.warning(f"Error notifying manager about date change: {tg_err}")

        # 5. Уведомление через WebSocket
        await manager.broadcast({
            "type": "DELIVERY_UPDATED",
            "payload": {"id": delivery.id, "delivery_date": str(new_date_obj)}
        })

        return {"status": "ok", "message": f"Дата успішно змінена на {new_date_obj}"}

    except Exception as e:
        logger.error(f"❌ Помилка зміни дати доставки: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Не вдалося змінити дату: {e}",
        )


@app.delete("/delivery/delete", dependencies=[Depends(check_not_guest)])
async def delete_delivery(data: DeleteDeliveryRequest):
    """
    Повністю видаляє доставку, пов'язані товари, подію в календарі та сповіщення в Telegram.
    """
    try:
        # 1. Отримуємо дані про доставку перед видаленням (для календаря)
        delivery = await Deliveries.objects().where(Deliveries.id == data.delivery_id).first().run()
        if not delivery:
            raise HTTPException(status_code=404, detail="Доставку не знайдено")

        # 2. Видаляємо сповіщення в Telegram
        await delete_delivery_notifications(data.delivery_id)

        # 3. Видаляємо з Google Calendar та таблиці Events
        if delivery.calendar_id:
            try:
                delete_calendar_event_by_id(delivery.calendar_id)
                await Events.delete().where(Events.event_id == delivery.calendar_id).run()
            except Exception as cal_err:
                logger.error(f"Error deleting calendar event: {cal_err}")

        # 4. Видаляємо товари та саму доставку (Piccolo не видаляє каскадно автоматично без налаштувань)
        async with Deliveries._meta.db.transaction():
            await DeliveryItems.delete().where(DeliveryItems.delivery == data.delivery_id).run()
            await Deliveries.delete().where(Deliveries.id == data.delivery_id).run()

        # 5. Уведомление через WebSocket
        await manager.broadcast({
            "type": "DELIVERY_DELETED",
            "payload": {"id": data.delivery_id}
        })

        logger.info(f"🗑 Доставка ID: {data.delivery_id} ({delivery.client}) повністю видалена.")
        return {"status": "ok", "message": "Доставка успішно видалена"}

    except Exception as e:
        logger.error(f"❌ Помилка видалення доставки: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Не вдалося видалити доставку: {e}",
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
