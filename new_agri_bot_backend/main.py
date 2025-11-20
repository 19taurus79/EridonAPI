import csv
import json
import io
import os
import tempfile
import uuid
from pathlib import Path
from typing import Optional, List, Dict

import pandas as pd
import uvicorn
import asyncio
from concurrent.futures import ThreadPoolExecutor

from piccolo.columns.defaults import TimestampNow
from . import models, processing
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
from .bi import router as bi_router
from .bi_pandas import router as bi_pandas_router
from .utils import send_message_to_managers

# Импорт TELEGRAM_BOT_TOKEN из config.py для инициализации бота
from .config import TELEGRAM_BOT_TOKEN, bot

# Инициализация Telegram Bot (используется в utils.py, но может быть нужен здесь для глобальной инициализации)
from aiogram import Bot
from google.oauth2 import service_account
from googleapiclient.discovery import build
from pydantic import BaseModel
from datetime import date

# Важно: если бот не используется напрямую в main, эту строку можно убрать


class ChangeDateRequest(BaseModel):
    new_date: date


class DeliveryItem(BaseModel):
    product: str
    quantity: float


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

sessions = {}


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
    "https://geocode-six.vercel.app",
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

    try:
        selected_moved = current_moved_df.loc[match_input.selected_moved_indices]
        selected_notes = current_notes_df.loc[match_input.selected_notes_indices]
    except KeyError:
        raise HTTPException(
            status_code=400,
            detail="Ошибка: одна или несколько выбранных позиций уже были сопоставлены ранее.",
        )

    sum_moved = selected_moved["Перемещено"].sum()
    sum_notes = selected_notes["Количество_в_примечании"].sum()
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

    # Проходим по каждой выбранной "перемещенной" позиции
    for _, moved_row in selected_moved.iterrows():
        # Создаем новую сопоставленную запись
        record = moved_row.to_dict()
        record["Договор"] = main_contract
        # Количество берем из "перемещено", так как это "факт"
        record["Количество"] = moved_row["Перемещено"]
        record["Источник"] = "Ручное сопоставление"
        newly_matched.append(record)

    # Обновляем состояние сессии: удаляем все выбранные пользователем позиции
    # из списков для дальнейшего сопоставления.
    try:
        current_moved_df.drop(match_input.selected_moved_indices, inplace=True)
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
