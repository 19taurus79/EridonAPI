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
from openpyxl.styles import Font, Alignment, PatternFill
from openpyxl.utils import get_column_letter
from asyncpg import UniqueViolationError
from piccolo.columns.defaults import TimestampNow
from . import models, processing
from .calendar_utils import (
    changed_color_calendar_events_by_id,
    delete_calendar_event_by_id,
    changed_date_calendar_events_by_id,
)
from .models import RegionResponse, AddressResponse, AddressCreate
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
)
from aiogram.types import FSInputFile, BufferedInputFile
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

from openpyxl import Workbook

# from openpyxl.utils import get_column_letter
from pydantic import BaseModel, Field, validator
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
from openpyxl.utils import get_column_letter

# Импорты из ваших новых модулей
from .telegram_auth import (
    router as telegram_auth_router,
    InitDataModel,
    check_telegram_auth,
    get_current_telegram_user,
)
from .data_retrieval import router as data_retrieval_router
from .data_loader import save_processed_data_to_db
from .bi import router as bi_router
from .bi_pandas import router as bi_pandas_router
from .order_chat import router as chat_router
from .notification import router as notification_router
from .utils import send_message_to_managers, create_composite_key_from_dict

# Импорт TELEGRAM_BOT_TOKEN из config.py для инициализации бота
from .config import TELEGRAM_BOT_TOKEN, bot

# Инициализация Telegram Bot (используется в utils.py, но может быть нужен здесь для глобальной инициализации)
from aiogram import Bot, Dispatcher
from aiogram.filters import CommandStart
from aiogram.types import Update
from .telegram_auth import confirm_login_token
from google.oauth2 import service_account
from googleapiclient.discovery import build
from pydantic import BaseModel
from datetime import date

# Важно: если бот не используется напрямую в main, эту строку можно убрать


class ChangeDateRequest(BaseModel):
    new_date: date


class Party(BaseModel):
    moved_q: float
    party: str


class DeliveryItem(BaseModel):
    product: str
    quantity: float
    weight: float
    parties: List[Party]


class DeliveryOrder(BaseModel):
    order: str
    items: List[DeliveryItem]


class DeleteDeliveryRequest(BaseModel):
    delivery_id: int


class DeliveryRequest(BaseModel):
    client: str
    manager: str
    address: str
    contact: str
    phone: str
    date: str  # ISO-формат строки
    comment: str
    is_custom_address: bool
    latitude: float
    longitude: float
    total_weight: float
    orders: List[DeliveryOrder]


class UpdateParty(BaseModel):
    party: str
    moved_q: float


class UpdateItem(BaseModel):
    product: str
    nomenclature: str
    quantity: float
    manager: str
    client: str
    order_ref: Optional[str] = Field(None, alias="orderRef")
    weight: float
    parties: List[UpdateParty]


class UpdateDeliveryRequest(BaseModel):
    delivery_id: int
    status: str
    items: List[UpdateItem]


class ChangeDeliveryDateRequest(BaseModel):
    delivery_id: int
    new_date: str


class CommentType(str, Enum):
    """Тип коментаря"""

    ORDER = "order"
    PRODUCT = "product"


class CreateCommentRequest(BaseModel):
    """Запит на створення коментаря"""

    comment_type: CommentType = Field(
        ..., description="Тип коментаря: order або product"
    )
    order_ref: str = Field(..., min_length=1, max_length=50, description="Номер заявки")
    product_id: Optional[str] = Field(None, description="UUID товару (для дашборду)")
    product_name: Optional[str] = Field(
        None, max_length=255, description="Назва товару (для BI)"
    )
    comment_text: str = Field(..., min_length=1, description="Текст коментаря")

    @validator("comment_text")
    def validate_comment_text(cls, v):
        """Валідація тексту коментаря"""
        if not v or not v.strip():
            raise ValueError("Текст коментаря не може бути порожнім")
        return v.strip()

    @validator("product_id", "product_name")
    def validate_product_fields(cls, v, values):
        """Валідація полів товару залежно від типу коментаря"""
        comment_type = values.get("comment_type")

        # Для коментарів заявки product_id та product_name мають бути None
        if comment_type == CommentType.ORDER and v is not None:
            raise ValueError(
                "Для коментарів заявки product_id та product_name мають бути null"
            )

        return v

    @validator("product_name")
    def validate_product_comment(cls, v, values):
        """Для коментарів товару хоча б одне поле має бути заповнене"""
        comment_type = values.get("comment_type")
        product_id = values.get("product_id")

        if comment_type == CommentType.PRODUCT:
            if not product_id and not v:
                raise ValueError(
                    "Для коментарів товару product_id або product_name обов'язкові"
                )

        return v

    class Config:
        json_schema_extra = {
            "example": {
                "comment_type": "product",
                "order_ref": "ТЕ-00071300",
                "product_id": "9aa0c0fc-1239-42cb-a4ec-59c614d77423",
                "product_name": "Аклон 60%, к.с. (5 л)",
                "comment_text": "Потрібно терміново відвантажити",
            }
        }


class UpdateCommentRequest(BaseModel):
    """Запит на оновлення коментаря"""

    comment_text: str = Field(..., min_length=1, description="Новий текст коментаря")

    @validator("comment_text")
    def validate_comment_text(cls, v):
        if not v or not v.strip():
            raise ValueError("Текст коментаря не може бути порожнім")
        return v.strip()

    class Config:
        json_schema_extra = {"example": {"comment_text": "Оновлений текст коментаря"}}


class CommentResponse(BaseModel):
    """Відповідь з даними коментаря"""

    id: int = Field(..., description="ID коментаря")
    comment_type: CommentType = Field(..., description="Тип коментаря")
    order_ref: str = Field(..., description="Номер заявки")
    product_id: Optional[uuid.UUID] = Field(None, description="UUID товару")
    product_name: Optional[str] = Field(None, description="Назва товару")
    comment_text: str = Field(..., description="Текст коментаря")
    created_by: int = Field(..., description="Telegram ID автора")
    created_by_name: str = Field(..., description="Ім'я автора")
    created_at: datetime = Field(..., description="Дата створення")
    updated_at: Optional[datetime] = Field(None, description="Дата оновлення")

    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "id": 123,
                "comment_type": "product",
                "order_ref": "ТЕ-00071300",
                "product_id": "9aa0c0fc-1239-42cb-a4ec-59c614d77423",
                "product_name": "Аклон 60%, к.с. (5 л)",
                "comment_text": "Потрібно терміново відвантажити",
                "created_by": 123456789,
                "created_by_name": "Іван Петренко",
                "created_at": "2026-02-02T12:00:00.000Z",
                "updated_at": None,
            }
        }


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SERVICE_ACCOUNT_FILE = os.path.join(BASE_DIR, "credentials.json")
SCOPES = ["https://www.googleapis.com/auth/calendar"]
CALENDAR_ID = "dca9aa4129540be8ec133f20092e7f0a500897595fc1736cd295a739d9dc9466@group.calendar.google.com"  # или укажи явный ID календаря

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


async def create_calendar_event(data: DeliveryRequest) -> Optional[str]:
    try:
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES
        )
        service = build("calendar", "v3", credentials=credentials)

        delivery_date = datetime.strptime(data.date, "%Y-%m-%d")
        start = delivery_date.replace(hour=9, minute=0)
        end = start + timedelta(hours=1)

        # 📝 Основная информация
        lines = [
            f"Контрагент: {data.client}",
            f"Менеджер: {data.manager}",
            f"Адреса: {data.address}",
            f"Контакт: {data.contact}",
            f"Телефон: {data.phone}",
            f"Дата доставки: {data.date}",
            f"Коментар : {data.comment}",
            "",
        ]

        # 📦 Добавляем заказы и товары
        for order in data.orders:
            lines.append(f"📦 Доповнення: {order.order}")
            for item in order.items:
                lines.append(f" • {item.product} — {item.quantity}")
            lines.append("")  # пустая строка между заказами

        description = "\n".join(lines)

        event = {
            "summary": f"🚚 Доставка: {data.client}",
            "location": data.address,
            "description": description,
            "start": {
                "dateTime": start.isoformat(),
                "timeZone": "Europe/Kyiv",
            },
            "end": {
                "dateTime": end.isoformat(),
                "timeZone": "Europe/Kyiv",
            },
            "colorId": "11",
        }

        created_event = (
            service.events().insert(calendarId=CALENDAR_ID, body=event).execute()
        )

        return created_event

    except Exception as e:
        print("Ошибка при добавлении в календарь:", e)
        return None


def get_calendar_events(
    start_date: Optional[str] = None, end_date: Optional[str] = None
) -> Optional[List[Dict]]:
    """
    Получает список событий из календаря в заданном диапазоне дат.

    Args:
        start_date (str, optional): Начальная дата в формате 'YYYY-MM-DD'.
        end_date (str, optional): Конечная дата в формате 'YYYY-MM-DD'.

    Returns:
        Optional[List[Dict]]: Список событий или None в случае ошибки.
    """
    try:
        # 1. Подключение к API
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES
        )
        service = build("calendar", "v3", credentials=credentials)

        # 2. Определение временного диапазона
        now = datetime.utcnow()
        time_min = (datetime.utcnow() - timedelta(days=3)).replace(
            hour=0, minute=0, second=0, microsecond=0
        ).isoformat() + "Z"  # По умолчанию за последние 7 дней
        time_max = (now + timedelta(days=3)).replace(
            hour=23, minute=59, second=0, microsecond=0
        ).isoformat() + "Z"  # До текущего момента

        if start_date:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            time_min = (
                start_dt.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
                + "Z"
            )

        if end_date:
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            time_max = (
                end_dt.replace(hour=23, minute=59, second=0, microsecond=0).isoformat()
                + "Z"
            )

        # 3. Выполнение запроса к API
        events_result = (
            service.events()
            .list(
                calendarId=CALENDAR_ID,
                timeMin=time_min,
                timeMax=time_max,
                # maxResults=20,  # Максимальное количество событий
                singleEvents=True,
                orderBy="startTime",
            )
            .execute()
        )

        events = events_result.get("items", [])
        return events

    except Exception as e:
        print("Ошибка при получении событий из календаря:", e)
        return None


def get_calendar_events_by_id(id: str):
    """
    Получает список событий из календаря в заданном диапазоне дат.

    Args:
        start_date (str, optional): Начальная дата в формате 'YYYY-MM-DD'.
        end_date (str, optional): Конечная дата в формате 'YYYY-MM-DD'.

    Returns:
        Optional[List[Dict]]: Список событий или None в случае ошибки.
    """
    try:
        # 1. Подключение к API
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES
        )
        service = build("calendar", "v3", credentials=credentials)

        # 3. Выполнение запроса к API
        events_result = (
            service.events()
            .get(
                calendarId=CALENDAR_ID,
                eventId=id,
            )
            .execute()
        )
        # print(events_result)
        # events = events_result.get("items", [])
        return events_result

    except Exception as e:
        print("Ошибка при получении событий из календаря:", e)
        return None


BACKEND_URL = os.getenv("BACKEND_URL", "")

# aiogram Dispatcher для обработки входящих сообщений бота
dp = Dispatcher()


@dp.message(CommandStart())
async def handle_bot_start(message):
    """Handle /start weblogin_TOKEN — Bot Deep Link Auth."""
    text = message.text or ""
    parts = text.split(" ", 1)
    if len(parts) == 2 and parts[1].startswith("weblogin_"):
        token = parts[1][len("weblogin_"):]
        telegram_id = message.from_user.id
        success = await confirm_login_token(token, telegram_id)
        if success:
            await message.answer(
                "✅ Вхід підтверджено! Поверніться в браузер — сторінка завантажиться автоматично."
            )
        else:
            await message.answer(
                "❌ Посилання не знайдено або вже використано. Спробуйте ще раз."
            )
    else:
        await message.answer("Вітаю! Я бот авторизації Eridon.")


# Определяем контекстный менеджер для жизненного цикла приложения
@asynccontextmanager
async def lifespan(app: FastAPI):
    print(
        "Piccolo database engine initialized. Connections will be managed automatically."
    )
    # Регистрируем webhook для бота если есть BACKEND_URL
    if BACKEND_URL:
        webhook_url = f"{BACKEND_URL}/webhook/bot"
        try:
            await bot.set_webhook(webhook_url)
            print(f"Telegram webhook registered: {webhook_url}")
        except Exception as e:
            print(f"Failed to set webhook: {e}")
    yield
    # Удаляем webhook при остановке
    if BACKEND_URL:
        try:
            await bot.delete_webhook()
            print("Telegram webhook removed.")
        except Exception:
            pass
    print("Piccolo database engine shutdown. Connections are closed automatically.")



app = FastAPI(
    title="Data Loader API for Agri-Bot",
    description="API for loading and processing various Excel data into PostgreSQL.",
    version="1.0.0",
    lifespan=lifespan,
)

# --- Конфигурация CORS ---
origins = [
    "https://taurus.pp.ua",
    "https://eridon-react.vercel.app",
    "https://eridon-bot-next-js.vercel.app",
    "http://127.0.0.1:5173",
    "http://localhost:5173",
    "http://127.0.0.1:5500",
    "http://127.0.0.1:8000",
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "https://telegram-mini-app-six-inky.vercel.app",
    "https://geocode-six.vercel.app",
    "https://paravail-aubrianna-noncrystalline.ngrok-free.dev",
    "http://eridon-dev.local",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Подключение маршрутов ---
app.include_router(telegram_auth_router)  # Подключаем маршруты из telegram_auth.py
app.include_router(data_retrieval_router)
app.include_router(bi_router)
app.include_router(bi_pandas_router)
app.include_router(chat_router)
app.include_router(notification_router)
app.mount("/admin", admin_router)


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
class Product(BaseModel):
    product: str
    quantity: int


class Order(BaseModel):
    order: str
    products: List[Product]


class ClientData(BaseModel):
    client: str
    manager: str
    orders: List[Order]
    deliveryAddress: Optional[str]
    contactPerson: Optional[str]
    deliveryDate: Optional[str]


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


def json_to_csv_save_local_d_drive(data: List[ClientData]) -> str:
    filepath = Path("D:/orders.csv")

    with open(filepath, mode="w", encoding="utf-8", newline="") as f:
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

    return str(filepath)  # возвращаем путь к файлу


@app.post("/send_telegram_message/")
async def send_telegram_message(
    data: List[ClientData],
    chat_id: int = Query(..., description="Telegram chat id для отправки сообщения"),
):
    message_text = format_message(data)
    csv_file = json_to_csv_save_local_d_drive(data)

    try:
        # await bot.send_message(chat_id=chat_id, text=message_text, parse_mode="HTML")
        await bot.send_document(
            chat_id=chat_id, document=FSInputFile(csv_file, filename=csv_file.name)
        )
        return {"status": "ok", "message": "Сообщение и CSV файл отправлены"}
    except Exception as e:
        return {"status": "error", "details": str(e)}


class TelegramMessage(BaseModel):  # ← ДОБАВЬ ЭТО
    chat_id: int  # ← ТВОИ поля из RN
    text: str


@app.post("/send_telegram_message_by_event")
async def message(message: TelegramMessage):
    await bot.send_message(
        text=message.text, chat_id=message.chat_id, parse_mode="HTML"
    )


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
        print(
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
        print(
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
    print(f"[{datetime.now(timezone.utc)}] Получен запрос на загрузку данных.")

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
        print(f"Ошибка при обработке загруженных файлов: {e}")
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
        print(f"--- Ошибка при запросе среднего веса: {e} ---")

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


@app.put("/update_address_for_client/{id}")
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
    # Обновляем поля из словаря data
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


@app.post("/add_address_for_client")
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


@app.post("/delivery/send")
async def send_delivery(data: DeliveryRequest, X_Telegram_Init_Data: str = Header()):
    parsed_init_data = check_telegram_auth(X_Telegram_Init_Data)
    user_info_str = parsed_init_data.get("user")
    user_data = json.loads(user_info_str)
    telegram_id = user_data.get("id")
    # ----------------------------Сообщение для Телеграм-----------------------
    # 📝 Формируем текст для Telegram
    print(X_Telegram_Init_Data)
    message_lines = [
        f"👤 Менеджер: {data.manager}",
        f"🚚 Контрагент: <code>{data.client}</code>",
        f"📍 Адреса: {data.address}",
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
            # Обрати внимание: я заменил "└" на "│" у товара,
            # чтобы визуально связать его с партиями ниже, если они есть.
            # Если партий нет — это можно подправить, но пока оставим так для связности.

            # Отбираем только партии с движением
            active_parties = [p for p in item.parties if p.moved_q > 0]

            # Считаем сколько их всего
            count = len(active_parties)

            if count > 0:
                for i, party in enumerate(active_parties):
                    # Проверяем: это последняя партия в списке?
                    is_last = i == count - 1

                    # Если последняя - ставим "уголок" (└), иначе "тройник" (├)
                    branch_symbol = "└" if is_last else "├"

                    message_lines.append(
                        f"   {branch_symbol} 🔖 <code>{party.party}</code>: {party.moved_q} шт."
                    )
            else:
                # Если партий нет, закрываем ветку товара красиво (опционально)
                pass

            message_lines.append("")

        message_lines.append("════════════════════")
        message_lines.append("")
        message = "\n".join(message_lines)
    # ------------------------------------------------------------------------------

    # ---------------------Формирование файла Excel---------------------------------
    wb = Workbook()
    ws = wb.active
    ws.title = "Доставка"

    # Заголовок документа (жирный, по центру)
    header_font = Font(bold=True, size=14)
    ws.append(["Менеджер", data.manager])
    ws["A1"].font = header_font
    ws["B1"].font = Font(bold=True)

    ws.append(["Контрагент", data.client])
    ws["A2"].font = header_font
    ws["B2"].font = Font(bold=True)

    ws.append(["Адреса", data.address])
    ws["A3"].font = header_font
    ws["B3"].font = Font(bold=True)

    ws.append(["Контакт", data.contact])
    ws["A4"].font = header_font
    ws["B4"].font = Font(bold=True)

    ws.append(["Телефон", data.phone])
    ws["A5"].font = header_font
    ws["B5"].font = Font(bold=True)

    ws.append(["Дата", data.date])
    ws["A6"].font = header_font
    ws["B6"].font = Font(bold=True)

    ws.append(["Коментар", data.comment or ""])
    ws["A7"].font = header_font
    ws["B7"].font = Font(bold=True)

    # Пустая строка
    ws.append([])

    # Заголовок таблицы (с сеткой)
    header_fill = PatternFill(start_color="DDEBF7", fill_type="solid")
    title_font = Font(bold=True, size=12)
    thin_border = Border(
        left=Side(style="thin"),
        right=Side(style="thin"),
        top=Side(style="thin"),
        bottom=Side(style="thin"),
    )

    ws.append(["Доповнення", "Товар", "Кількість"])
    row = ws.max_row
    for col in range(1, 4):
        cell = ws.cell(row=row, column=col)
        cell.font = title_font
        cell.fill = header_fill
        cell.alignment = Alignment(horizontal="center")
        cell.border = thin_border

    # Таблица товаров
    for order in data.orders:
        for item in order.items:
            # ОСНОВНАЯ СТРОКА - ВСЕ ЖИРНОЕ, количество ВПРАВО
            ws.append([order.order, item.product, item.quantity])
            main_row = ws.max_row

            # Стили основной строки
            main_bold_font = Font(bold=True)
            ws[f"A{main_row}"].font = main_bold_font
            ws[f"A{main_row}"].alignment = Alignment(horizontal="left")

            ws[f"B{main_row}"].font = main_bold_font
            ws[f"B{main_row}"].alignment = Alignment(horizontal="left")

            ws[f"C{main_row}"].font = main_bold_font
            ws[f"C{main_row}"].alignment = Alignment(
                horizontal="right"
            )  # Количество ВПРАВО

            # Границы основной строки
            for col in range(1, 4):
                ws.cell(row=main_row, column=col).border = thin_border

            # Подстроки партий - обычный шрифт, количество ВЛЕВО
            if item.parties and item.parties[0].moved_q > 0:
                for party in item.parties:
                    ws.append(["", f"  ↳ {party.party}", party.moved_q])
                    party_row = ws.max_row

                    # Партия: обычный шрифт, ВЛЕВО
                    party_font = Font(italic=True, size=11)
                    ws[f"B{party_row}"].font = party_font
                    ws[f"B{party_row}"].alignment = Alignment(horizontal="left")

                    ws[f"C{party_row}"].font = party_font  # НЕ жирный, как название
                    ws[f"C{party_row}"].alignment = Alignment(
                        horizontal="left"
                    )  # ВЛЕВО

                    # Границы партии
                    for col in range(1, 4):
                        ws.cell(row=party_row, column=col).border = thin_border

    # Двойная линия снизу таблицы
    last_row = ws.max_row
    for col in range(1, 4):
        ws.cell(row=last_row, column=col).border = Border(
            left=Side(style="thin"),
            right=Side(style="thin"),
            top=Side(style="thin"),
            bottom=Side(style="double"),
        )

    # Автоподбор ширины
    for column in ws.columns:
        max_length = 0
        column_letter = get_column_letter(column[0].column)
        for cell in column:
            try:
                if len(str(cell.value)) > max_length:
                    max_length = len(str(cell.value))
            except:
                pass
        adjusted_width = min(max_length + 2, 50)
        ws.column_dimensions[column_letter].width = adjusted_width

    # wb.save("доставка.xlsx")

    # Сохраняем Excel во временный файл
    # Название файла с именем менеджера
    safe_manager = data.manager.replace(" ", "_")
    filename = (
        f"Доставка_{safe_manager}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    )

    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
        # Сохраняем Excel
        wb.save(tmp.name)
        tmp.flush()

        # Проверяем окружение. Если не 'prod', выводим в консоль вместо отправки.
        app_env = os.getenv("APP_ENV", "dev")

        calendar = await create_calendar_event(data)
        # Создание события в календаре
        # calendar = await create_calendar_event(data)
        if calendar:
            calendar_link = calendar.get("htmlLink")
            date = datetime.fromisoformat(calendar["start"]["dateTime"]).date()
            await Events.insert(
                Events(
                    event_id=calendar["id"],
                    event_creator=telegram_id,
                    event_creator_name=data.manager,
                    event_status=0,
                    start_event=date,
                    event=data.client,
                )
            ).run()
            print("📅 Добавлено в календарь:", calendar_link)
        else:
            print("❌ Не удалось добавить в календарь")
        # --- ШАГ 1: Сохранение данных в БД (ВРЕМЕННО ВЫНЕСЕНО ДЛЯ ТЕСТА) ---
        try:
            # 1.1 Создаем основную запись о доставке
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
                created_by=telegram_id,
                calendar_id=calendar["id"],
            )
            await new_delivery.save().run()
            print(f"✅ Основна інформація по доставці ID: {new_delivery.id} збережена.")

            # 1.2 Готовим список товаров для массовой вставки
            items_to_insert = []
            for order in data.orders:
                for item in order.items:
                    # Проверяем, есть ли вообще партии для этого товара
                    if item.parties:
                        for party in item.parties:
                            # Добавляем только те партии, где есть движение
                            if party.moved_q > 0:
                                items_to_insert.append(
                                    DeliveryItems(
                                        delivery=new_delivery.id,  # Связь с основной записью
                                        order_ref=order.order,
                                        product=item.product,
                                        quantity=item.quantity,
                                        party=party.party,
                                        party_quantity=party.moved_q,
                                    )
                                )
            # 1.3 Сохраняем все товары одним запросом
            if items_to_insert:
                await DeliveryItems.insert(*items_to_insert).run()
                print(f"✅ {len(items_to_insert)} позицій по доставці збережено.")

        except Exception as e:
            print(f"❌ Помилка збереження доставки в БД: {e}")
            raise HTTPException(status_code=500, detail=f"Помилка збереження в БД: {e}")

        if app_env == "production":
            # Готовим файл к отправке
            excel_file = FSInputFile(tmp.name, filename=filename)

            # Отправка сообщения администраторам
            admins_json = os.getenv("ADMINS", "[]")
            admins = json.loads(admins_json)
            for admin in admins:
                await bot.send_message(chat_id=admin, text=message, parse_mode="HTML")
                await bot.send_document(chat_id=admin, document=excel_file)

            # Отправка сообщения пользователю
            await bot.send_message(
                chat_id=telegram_id, text="Ви відправили такі данні для доставки:"
            )
            await bot.send_message(chat_id=telegram_id, text=message, parse_mode="HTML")

            # Создание события в календаре
            # calendar = await create_calendar_event(data)
            # if calendar:
            #     calendar_link = calendar.get("htmlLink")
            #     date = datetime.fromisoformat(calendar["start"]["dateTime"]).date()
            #     await Events.insert(
            #         Events(
            #             event_id=calendar["id"],
            #             event_creator=telegram_id,
            #             event_creator_name=data.manager,
            #             event_status=0,
            #             start_event=date,
            #             event=data.client,
            #         )
            #     ).run()
            #     print("📅 Добавлено в календарь:", calendar_link)
            # else:
            #     print("❌ Не удалось добавить в календарь")

        else:
            # Режим разработки: выводим все в консоль
            print("\n--- [DEV] РЕЖИМ: ВІДПРАВКА ПОВІДОМЛЕННЯ ПРО ДОСТАВКУ ---")
            print(f"--- [DEV] Одержувачі (адміни): {os.getenv('ADMINS', '[]')}")
            print(f"--- [DEV] Одержувач (користувач): {telegram_id}")
            print("--- [DEV] Текст повідомлення: ---")
            print(message)
            print(f"--- [DEV] Excel-файл '{filename}' було б надіслано. ---")
            print("--- [DEV] Створення події в календарі пропущено. ---")

    # Удаляем временный файл
    os.remove(tmp.name)

    return {"status": "ok"}


@app.delete("/delivery/delete", tags=["Delivery"])
async def delete_delivery(deliveryId: DeleteDeliveryRequest):
    data = (
        await Deliveries.objects()
        .where(Deliveries.id == deliveryId.delivery_id)
        .first()
    )
    await bot.send_message(
        chat_id=data.created_by,
        text=(
            f"❌ <b>Доставку скасовано</b>\n\n"
            f"👤 Клієнт: <b>{data.client}</b>\n"
            f"🗑 <i>Дані про доставку видалено з бази.</i>"
        ),
        parse_mode="HTML",
    )
    await Deliveries.delete().where(Deliveries.id == deliveryId.delivery_id).run()
    await Events.delete().where(Events.event_id == data.calendar_id).run()
    delete_calendar_event_by_id(event_id=data.calendar_id)


@app.post("/delivery/update", tags=["Delivery"])
async def update_delivery(data: UpdateDeliveryRequest):
    """
    Оновлює доставку, повністю замінюючи її позиції однією транзакцією.
    """
    try:
        # Початок транзакції для забезпечення атомарності
        async with Deliveries._meta.db.transaction():
            # 1. Оновлення статусу доставки
            delivery_data = await Deliveries.objects().where(Deliveries.id == data.delivery_id).first()
            await Deliveries.update({Deliveries.status: data.status}).where(
                Deliveries.id == data.delivery_id
            ).run()
            # print(delivery_data)
            event_data = await Events.objects().where(Events.event_id == delivery_data.calendar_id).first()
            # print(event_data)
            calendar_data = get_calendar_events_by_id(delivery_data.calendar_id)
            if delivery_data.status == data.status:
                print(f"⚠️ Статус доставки ID: {data.delivery_id} вже має значення '{data.status}'. Тому повідомлення не відправляється, а статус оновлюється в базі. Створення події в календарі пропущено.")
                
            elif delivery_data.status == 'Виконано' and data.status == 'В роботі':
                print("Скоріш за все відміна виконання доставки, тому повідомлення не відправляється, а статус просто оновлюється в базі.")
            else:
                if data.status == 'Виконано':
                    await bot.send_message(
                        chat_id=delivery_data.created_by,
                        text=(
                            f"🎉 <b>Доставку виконано</b>\n\n"
                            f"👤 Клієнт: <b>{delivery_data.client}</b>\n"
                        ),
                        parse_mode="HTML",
                    )
                    changed_color_calendar_events_by_id(id=delivery_data.calendar_id,status=2)
                    await Events.update({Events.event_status: 2}).where(Events.event_id == delivery_data.calendar_id).run()
                elif data.status == 'В роботі':
                    await bot.send_message(
                        chat_id=delivery_data.created_by,
                        text=(
                            f"✅ <b>Доставка в роботі</b>\n\n"
                            f"👤 Клієнт: <b>{delivery_data.client}</b>\n"
                            f"Дані по доставці передані бухгалтеру, та будуть передані на склад для комплектації\n"),
                        parse_mode="HTML",
                    )
                    changed_color_calendar_events_by_id(id=delivery_data.calendar_id,status=1)
                    await Events.update({Events.event_status: 1}).where(Events.event_id == delivery_data.calendar_id).run()

            print(
                f"✅ Статус доставки ID: {data.delivery_id} оновлено на '{data.status}'."
            )

            # 2. Видалення всіх існуючих позицій для цієї доставки
            await DeliveryItems.delete().where(
                DeliveryItems.delivery == data.delivery_id
            ).run()

            # 3. Підготовка нових позицій для масової вставки
            items_to_insert = []
            for item in data.items:
                if item.parties:
                    for party in item.parties:
                        # Додаємо позицію для кожної партії
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
                    # Обробка позицій без партій, якщо необхідно
                    items_to_insert.append(
                        DeliveryItems(
                            delivery=data.delivery_id,
                            order_ref=item.order_ref,
                            product=item.product,
                            quantity=item.quantity,
                        )
                    )

            # 4. Виконання масової вставки для всіх нових позицій
            if items_to_insert:
                await DeliveryItems.insert(*items_to_insert).run()
            else:
                # Якщо товарів немає, видаляємо саму доставку
                await Deliveries.delete().where(Deliveries.id == data.delivery_id).run()
                print(
                    f"🗑️ Доставка ID: {data.delivery_id} видалена, бо в ній не залишилось товарів."
                )
                return {
                    "status": "ok",
                    "message": "Delivery deleted as it became empty.",
                }

    except Exception as e:
        # Якщо будь-який крок завершується невдачею, транзакція буде автоматично відкочена.
        print(f"❌ Помилка оновлення доставки: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Не вдалося оновити позиції доставки: {e}",
        )

    return {"status": "ok", "message": "Delivery items updated successfully."}


@app.post("/delivery/change_date", tags=["Delivery"])
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

        print(f"✅ Дата доставки ID: {data.delivery_id} оновлена з {old_date} на {new_date_obj}.")
        return {"status": "ok", "message": "Delivery date updated successfully."}

    except Exception as e:
        print(f"❌ Помилка оновлення дати доставки: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Не вдалося оновити дату доставки: {e}",
        )


@app.post(
    "/orders/comments/create",
    response_model=CommentResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Створити коментар",
    description="Створює новий коментар до заявки або товару",
    dependencies=[Depends(get_current_telegram_user)],
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
        print(f"❌ Помилка створення коментаря: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Не вдалося зберегти коментар: {e}",
        )


@app.get(
    "/orders/comments/list",
    response_model=List[CommentResponse],
    summary="Отримати коментарі",
    description="Отримує всі коментарі для вказаної заявки",
)
async def get_comments(
    order_ref: str = Query(..., description="Номер заявки"),
    # user: dict = Depends(get_current_telegram_user)
):
    """
    Отримання всіх коментарів для заявки

    - **order_ref**: Номер заявки

    Повертає список коментарів, відсортованих за датою створення (найновіші спочатку)
    """

    if not order_ref or not order_ref.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="order_ref обов'язковий параметр",
        )
    comments = (
        await OrderComments.select()
        .where(OrderComments.order_ref == order_ref)
        .order_by(OrderComments.created_at, ascending=False)
        .run()
    )

    return comments


@app.put(
    "/orders/comments/{comment_id}",
    response_model=CommentResponse,
    summary="Оновити коментар",
    description="Оновлює текст коментаря (тільки власник може редагувати)",
    dependencies=[Depends(get_current_telegram_user)],
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
    dependencies=[Depends(get_current_telegram_user)],
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
