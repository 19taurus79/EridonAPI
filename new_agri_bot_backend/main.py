import csv
import json
import io
import os
import tempfile
from pathlib import Path
from typing import Optional, List, Dict
import uvicorn
import asyncio
from concurrent.futures import ThreadPoolExecutor

from piccolo.columns.defaults import TimestampNow

from .tables import Remains, Events
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
    Request,
    Header,
)
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timezone, timedelta
from piccolo_admin.endpoints import create_admin

from openpyxl import Workbook
from openpyxl.utils import get_column_letter
from pydantic import BaseModel

# Импорты из ваших новых модулей
from .telegram_auth import (
    router as telegram_auth_router,
    InitDataModel,
    check_telegram_auth,
)
from .data_retrieval import router as data_retrieval_router
from .data_loader import save_processed_data_to_db
from .utils import send_message_to_managers

# Импорт TELEGRAM_BOT_TOKEN из config.py для инициализации бота
from .config import TELEGRAM_BOT_TOKEN

# Инициализация Telegram Bot (используется в utils.py, но может быть нужен здесь для глобальной инициализации)
from aiogram import Bot
from google.oauth2 import service_account
from googleapiclient.discovery import build
from pydantic import BaseModel
from datetime import date

bot = Bot(
    TELEGRAM_BOT_TOKEN
)  # Важно: если бот не используется напрямую в main, эту строку можно убрать


class ChangeDateRequest(BaseModel):
    new_date: date


class DeliveryItem(BaseModel):
    product: str
    quantity: int


class DeliveryOrder(BaseModel):
    order: str
    items: List[DeliveryItem]


class DeliveryRequest(BaseModel):
    client: str
    manager: str
    address: str
    contact: str
    phone: str
    date: str  # ISO-формат строки
    comment: str
    orders: List[DeliveryOrder]



BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SERVICE_ACCOUNT_FILE = os.path.join(BASE_DIR, "credentials.json")
SCOPES = ["https://www.googleapis.com/auth/calendar"]
CALENDAR_ID = "dca9aa4129540be8ec133f20092e7f0a500897595fc1736cd295a739d9dc9466@group.calendar.google.com"  # или укажи явный ID календаря

admin_router = create_admin([Remains], allowed_hosts=["localhost"])


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


# Определяем контекстный менеджер для жизненного цикла приложения
@asynccontextmanager
async def lifespan(app: FastAPI):
    print(
        "Piccolo database engine initialized. Connections will be managed automatically."
    )
    yield
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
app.mount("/admin", admin_router)


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


# --- Маршрут для загрузки и обработки данных ---
@app.post(
    "/upload-data",
    summary="Загрузить и обработать Excel-файлы",
    response_description="Статус загрузки данных и уведомление",
)
async def upload_data(
    background_tasks: BackgroundTasks,
    av_stock_file: UploadFile = File(..., description="Файл с доступными остатками"),
    remains_file: UploadFile = File(..., description="Файл с остатками"),
    submissions_file: UploadFile = File(..., description="Файл с заявками"),
    payment_file: UploadFile = File(..., description="Файл с оплатой"),
    moved_file: UploadFile = File(..., description="Файл с перемещенными данными"),
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
        moved_content = await moved_file.read()

        # Запускаем синхронную функцию обработки и сохранения в базу данных
        # в отдельном потоке, чтобы не блокировать ASGI-сервер.
        background_tasks.add_task(
            save_processed_data_to_db,  # Передаем функцию
            av_stock_content,
            remains_content,
            submissions_content,
            payment_content,
            moved_content,
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


@app.post("/delivery/send")
async def send_delivery(data: DeliveryRequest, X_Telegram_Init_Data: str = Header()):
    parsed_init_data = check_telegram_auth(X_Telegram_Init_Data)
    user_info_str = parsed_init_data.get("user")
    user_data = json.loads(user_info_str)
    telegram_id = user_data.get("id")
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
        message_lines.append(f"📦 *Доповнення:* <code>{order.order}</code>")
        for item in order.items:
            message_lines.append(f" • <code>{item.product}</code> — {item.quantity}")
        message_lines.append("")

    message = "\n".join(message_lines)

    # 🧾 Генерируем Excel
    wb = Workbook()
    ws = wb.active
    ws.title = "Доставка"

    ws.append(["Менеджер", data.manager])
    ws.append(["Контрагент", data.client])
    ws.append(["Адреса", data.address])
    ws.append(["Контакт", data.contact])
    ws.append(["Телефон", data.phone])
    ws.append(["Дата", data.date])
    ws.append(["Коментар", data.comment])
    ws.append([])
    ws.append(["Доповнення", "Товар", "Кількість"])

    for order in data.orders:
        for item in order.items:
            ws.append([order.order, item.product, item.quantity])

        # Сохраняем Excel во временный файл
    # Название файла с именем менеджера
    safe_manager = data.manager.replace(" ", "_")
    filename = (
        f"Доставка_{safe_manager}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    )
    # 📐 Устанавливаем автоширину колонок
    for column_cells in ws.columns:
        max_length = 0
        column = column_cells[0].column
        col_letter = get_column_letter(column)
        for cell in column_cells:
            if cell.value:
                max_length = max(max_length, len(str(cell.value)))
        ws.column_dimensions[col_letter].width = max_length + 2

    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
        # Сохраняем Excel
        wb.save(tmp.name)
        tmp.flush()

        # Готовим файл к отправке
        excel_file = FSInputFile(tmp.name, filename=filename)

        # Отправка сообщения
        # admins = ["548019148", "1060393824", "7953178333"]
        # admins = ["548019148", "1060393824"]
        admins_json = os.getenv("ADMINS", "[]")
        admins = json.loads(admins_json)
        for admin in admins:
            await bot.send_message(chat_id=admin, text=message, parse_mode="HTML")
            await bot.send_document(chat_id=admin, document=excel_file)
        await bot.send_message(
            chat_id=telegram_id, text="Ви відправили такі данні для доставки :"
        )
        await bot.send_message(chat_id=telegram_id, text=message, parse_mode="HTML")

    # Удаляем временный файл
    os.remove(tmp.name)

    calendar = await create_calendar_event(data)
    calendar_link = calendar["htmlLink"]
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

    if calendar_link:
        print("📅 Добавлено в календарь:", calendar_link)
    else:
        print("❌ Не удалось добавить в календарь")


    return {"status": "ok"}
