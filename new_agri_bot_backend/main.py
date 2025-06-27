from typing import Optional
import uvicorn
import asyncio
from concurrent.futures import ThreadPoolExecutor

from fastapi import (
    FastAPI,
    UploadFile,
    File,
    HTTPException,
    status,
    BackgroundTasks,
    Depends,
    Query,
)
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timezone, timedelta

# Импорты из ваших новых модулей
from .telegram_auth import router as telegram_auth_router, InitDataModel
from .data_retrieval import router as data_retrieval_router
from .data_loader import save_processed_data_to_db
from .utils import send_message_to_managers

# Импорт TELEGRAM_BOT_TOKEN из config.py для инициализации бота
from .config import TELEGRAM_BOT_TOKEN

# Инициализация Telegram Bot (используется в utils.py, но может быть нужен здесь для глобальной инициализации)
from aiogram import Bot

bot = Bot(
    TELEGRAM_BOT_TOKEN
)  # Важно: если бот не используется напрямую в main, эту строку можно убрать


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
    "http://127.0.0.1:8000",
    "http://localhost:3000",
    "http://127.0.0.1:3000",
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
