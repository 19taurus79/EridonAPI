# data_loader_api/app/main.py
from typing import Optional

import uvicorn
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
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.responses import JSONResponse
import pandas as pd
import io
import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import uuid
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware

# Импорты Piccolo и конфига
# from database import DB
from .tables import (
    ProductGuide,
    Remains,
    AvailableStock,
    Submissions,
    Payment,
    MovedData,
)
from .config import (
    TELEGRAM_BOT_TOKEN,
    MANAGERS_ID,
    valid_line_of_business,
    valid_warehouse,
)
import math
from datetime import datetime, date
from pydantic import BaseModel
import jwt
import os
from dotenv import load_dotenv

load_dotenv()
os.environ["PICCOLO_CONF"] = "piccolo_conf"
SECRET_KEY = os.getenv("SECRET_KEY")

# Алгоритм шифрования
ALGORITHM = "HS256"

# Время жизни токена
ACCESS_TOKEN_EXPIRE_MINUTES = os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", 30)

# Словарь для хранения пользователей
users = {
    os.getenv("POSTGRES_USER"): {
        "username": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
    }
}

# Словарь для хранения токенов
tokens = {}


# Определите модель для токена
class Token(BaseModel):
    access_token: str
    token_type: str


# Определите схему аутентификации
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


# Функция для создания токена
def create_access_token(data: dict, expires_delta: timedelta | None = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


# Функция для проверки токена
def verify_token(token: str):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")


# Функция для аутентификации пользователя
async def get_current_user(token: str = Depends(oauth2_scheme)):
    payload = verify_token(token)
    username = payload.get("sub")
    if username is None:
        raise HTTPException(status_code=401, detail="Could not validate credentials")
    user = users.get(username)
    if user is None:
        raise HTTPException(status_code=401, detail="User not found")
    return user


# Инициализация FastAPI приложения
# Определяем контекстный менеджер для жизненного цикла приложения
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Код, который выполняется при запуске приложения
    print(
        "Piccolo database engine initialized. Connections will be managed automatically."
    )
    # Здесь вы можете выполнить любые инициализационные задачи,
    # например, создать таблицы, если они не существуют, или проверить подключение.

    yield  # <-- Приложение запускается и обрабатывает запросы

    # Код, который выполняется при завершении работы приложения
    print("Piccolo database engine shutdown. Connections are closed automatically.")
    # Здесь вы можете выполнить любые задачи по очистке ресурсов,
    # например, закрыть файлы или другие соединения.


# Инициализация FastAPI приложения с lifespan
app = FastAPI(
    title="Data Loader API for Agri-Bot",
    description="API for loading and processing various Excel data into PostgreSQL.",
    version="1.0.0",
    lifespan=lifespan,  # <-- Передаем lifespan
)
# --- Конфигурация CORS ---
origins = [
    "https://taurus.pp.ua",  # Для http://localhost
    "https://eridon-react.vercel.app",  # Ваш React/Next.js сервер разработки
    "http://127.0.0.1:5173",  # На случай, если localhost разрешается в 127.0.0.1 для клиента
    "http://127.0.0.1:8000",  # Сам ваш бэкенд, если к нему иногда обращаются напрямую
    # Добавьте другие источники по мере необходимости для разных сред (например, ваш домен для staging/production)
    # "https://ваш-фронтенд-продакшен-домен.com",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # Список разрешенных источников
    allow_credentials=True,  # Разрешить куки (cookies) и HTTP-аутентификацию
    allow_methods=["*"],  # Разрешить все HTTP-методы (GET, POST, PUT, DELETE и т.д.)
    allow_headers=[
        "*"
    ],  # Разрешить все заголовки HTTP (Content-Type, Authorization и т.д.)
)
# --- Конец конфигурации CORS ---
# Инициализация Telegram Bot
from aiogram import Bot

bot = Bot(TELEGRAM_BOT_TOKEN)

# Пул потоков для выполнения синхронных операций (Pandas обработка)
# Это нужно, чтобы не блокировать основной асинхронный поток FastAPI,
# пока Pandas выполняет тяжелые вычисления.
executor = ThreadPoolExecutor(
    max_workers=4
)  # Можно настроить количество рабочих потоков


# Создайте маршрут для получения токена
@app.post("/token")
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    user = users.get(form_data.username)
    if not user:
        raise HTTPException(status_code=401, detail="Incorrect username or password")
    if not user["password"] == form_data.password:
        raise HTTPException(status_code=401, detail="Incorrect username or password")
    access_token_expires = timedelta(minutes=int(ACCESS_TOKEN_EXPIRE_MINUTES))
    access_token = create_access_token(
        data={"sub": user["username"]}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}


# Используйте аутентификацию для защищённых маршрутов
# @app.get("/protected")
# async def protected_route(user: dict = Depends(get_current_user)):
#     return {"message": f"Hello, {user['username']}"}


# Вспомогательная функция для чтения содержимого Excel в DataFrame
def read_excel_content(content: bytes, sheet_name=0) -> pd.DataFrame:
    # Используем io.BytesIO, чтобы Pandas мог читать из байтов в памяти
    return pd.read_excel(io.BytesIO(content), sheet_name=sheet_name)


# --- Функции для обработки каждого типа Excel-файла ---
# Эти функции принимают 'bytes' (сырое содержимое файла)
# и возвращают очищенный Pandas DataFrame.


def process_submissions(content: bytes) -> pd.DataFrame:
    submissions = read_excel_content(content)

    # Удаляем ненужные строки
    submissions.drop(axis=0, labels=[0, 1, 2, 3, 4, 5, 6, 7], inplace=True)
    submissions.drop(axis=0, labels=submissions.tail(1).index, inplace=True)

    # Удаляем ненужные колонки
    submissions.drop(
        axis=1, labels=["Unnamed: 1", "Unnamed: 2", "Unnamed: 6"], inplace=True
    )

    # Задаём правильные имена колонок
    submissions_col_names = [
        "division",
        "manager",
        "company_group",
        "client",
        "contract_supplement",
        "parent_element",
        "manufacturer",
        "active_ingredient",
        "nomenclature",
        "party_sign",
        "buying_season",
        "line_of_business",
        "period",
        "shipping_warehouse",
        "document_status",
        "delivery_status",
        "shipping_address",
        "transport",
        "plan",
        "fact",
        "different",
    ]
    submissions.columns = submissions_col_names

    # Преобразуем числовые колонки
    for col in ["plan", "fact", "different"]:
        submissions[col] = pd.to_numeric(submissions[col], errors="coerce").fillna(0)

    # Преобразуем текстовые колонки
    text_columns = [
        "division",
        "manager",
        "company_group",
        "client",
        "contract_supplement",
        "parent_element",
        "manufacturer",
        "active_ingredient",
        "nomenclature",
        "party_sign",
        "buying_season",
        "line_of_business",
        "shipping_warehouse",
        "document_status",
        "delivery_status",
        "shipping_address",
        "transport",
    ]

    for col in text_columns:
        submissions[col] = submissions[col].fillna("").astype(str)

    # Обновляем значения в колонке "party_sign"
    submissions.loc[
        submissions["party_sign"] == "Закупівля поточного сезону", "party_sign"
    ] = " "

    # Формируем колонку product
    submissions["product"] = submissions.apply(
        lambda row: f"{str(row['nomenclature']).rstrip()} {str(row['party_sign']).rstrip()} {str(row['buying_season']).rstrip()}",
        axis=1,
    )

    # Обрезаем contract_supplement
    submissions["contract_supplement"] = submissions["contract_supplement"].str.slice(
        23, 34
    )

    return submissions
    # submissions.drop(axis=0, labels=[0, 1, 2, 3, 4, 5, 6, 7], inplace=True)
    # submissions.drop(axis=0, labels=submissions.tail(1).index, inplace=True)
    # submissions.drop(
    #     axis=1, labels=["Unnamed: 1", "Unnamed: 2", "Unnamed: 6"], inplace=True
    # )

    # submissions_col_names = [
    #     "division", "manager", "company_group", "client", "contract_supplement",
    #     "parent_element", "manufacturer", "active_ingredient", "nomenclature",
    #     "party_sign", "buying_season", "line_of_business", "period",
    #     "shipping_warehouse", "document_status", "delivery_status",
    #     "shipping_address", "transport", "plan", "fact", "different",
    # ]
    # submissions.columns = submissions_col_names
    # submissions["plan"]=submissions["plan"].fillna(0)
    # submissions["fact"]=submissions["fact"].fillna(0)
    # submissions["different"]=submissions["different"].fillna(0)
    # submissions.fillna("", inplace=True)
    # submissions.loc[
    #     (submissions["party_sign"] == "Закупівля поточного сезону"), "party_sign"
    # ] = " "
    # submissions["product"] = submissions.apply(
    #     lambda row: str(row["nomenclature"]).rstrip()
    #                 + " "
    #                 + str(row["party_sign"]).rstrip()
    #                 + " "
    #                 + str(row["buying_season"]).rstrip(),
    #     axis=1,
    # )
    # submissions["contract_supplement"] = submissions["contract_supplement"].astype(str).str.slice(23, 34)
    return submissions


def process_av_stock(content: bytes) -> pd.DataFrame:
    av_stock = read_excel_content(content)
    # Удаляем ненужные строки и колонки
    av_stock.drop(axis=0, labels=[0, 1, 2, 3, 4, 5, 6], inplace=True)
    av_stock.drop(
        axis=1, labels=["Unnamed: 1", "Unnamed: 2", "Unnamed: 4"], inplace=True
    )

    # Новые имена колонок
    av_col_names = [
        "nomenclature",
        "party_sign",
        "buying_season",
        "division",
        "line_of_business",
        "active_substance",
        "available",
    ]
    av_stock.columns = av_col_names

    # Обработка текстовых колонок
    text_columns = [
        "nomenclature",
        "party_sign",
        "buying_season",
        "division",
        "line_of_business",
        "active_substance",
    ]

    for col in text_columns:
        av_stock[col] = av_stock[col].fillna("").astype(str)

    # Обработка числовой колонки (если она числовая)
    av_stock["available"] = pd.to_numeric(
        av_stock["available"], errors="coerce"
    ).fillna(0)

    # Формируем колонку product
    av_stock["product"] = av_stock.apply(
        lambda row: f"{row['nomenclature'].rstrip()} {row['party_sign'].rstrip()} {row['buying_season'].rstrip()}",
        axis=1,
    )

    return av_stock
    # av_stock.drop(axis=0, labels=[0, 1, 2, 3, 4, 5, 6], inplace=True)
    # av_stock.drop(
    #     axis=1, labels=["Unnamed: 1", "Unnamed: 2", "Unnamed: 4"], inplace=True
    # )
    # av_col_names = [
    #     "nomenclature", "party_sign", "buying_season", "division",
    #     "line_of_business", "active_substance", "available",
    # ]
    # av_stock.columns = av_col_names
    # av_stock.fillna("", inplace=True)
    # av_stock["product"] = av_stock.apply(
    #     lambda row: str(row["nomenclature"]).rstrip()
    #                 + " "
    #                 + str(row["party_sign"]).rstrip()
    #                 + " "
    #                 + str(row["buying_season"]).rstrip(),
    #     axis=1,
    # )
    # return av_stock


def process_remains_reg(content: bytes) -> pd.DataFrame:
    remains = read_excel_content(content)
    # Удаляем ненужные строки и колонки
    remains.drop(axis=0, labels=[0, 1, 2, 3, 4], inplace=True)
    remains.drop(
        axis=1, labels=["Unnamed: 1", "Unnamed: 2", "Unnamed: 4"], inplace=True
    )
    remains.drop(axis=0, labels=remains.tail(1).index, inplace=True)

    # Новые имена колонок
    remains_col_name = [
        "line_of_business",
        "warehouse",
        "parent_element",
        "nomenclature",
        "party_sign",
        "buying_season",
        "nomenclature_series",
        "mtn",
        "origin_country",
        "germination",
        "crop_year",
        "quantity_per_pallet",
        "active_substance",
        "certificate",
        "certificate_start_date",
        "certificate_end_date",
        "buh",
        "skl",
        "weight",
        "storage",
    ]
    remains.columns = remains_col_name
    # Удаляем столбец 'storage'
    remains.drop(columns=["storage"], inplace=True)

    # Обрабатываем числовые колонки
    for col in ["buh", "skl", "weight", "quantity_per_pallet"]:
        remains[col] = pd.to_numeric(remains[col], errors="coerce").fillna(0)

    # Обрабатываем текстовые колонки
    text_columns = [
        "line_of_business",
        "warehouse",
        "parent_element",
        "nomenclature",
        "party_sign",
        "buying_season",
        "nomenclature_series",
        "mtn",
        "origin_country",
        "germination",
        "crop_year",
        "active_substance",
        "certificate",
        "certificate_start_date",
        "certificate_end_date",
    ]
    for col in text_columns:
        remains[col] = remains[col].fillna("").astype(str)

    # Формируем колонку product
    remains["product"] = remains.apply(
        lambda row: f"{row['nomenclature'].rstrip()} {row['party_sign'].rstrip()} {row['buying_season'].rstrip()}",
        axis=1,
    )

    # Фильтруем по валидным значениям (предполагаю, что valid_line_of_business и valid_warehouse заданы где-то выше)
    remains = remains.loc[remains["line_of_business"].isin(valid_line_of_business)]
    remains = remains.loc[remains["warehouse"].isin(valid_warehouse)]

    return remains
    # remains.drop(axis=0, labels=[0, 1, 2, 3, 4], inplace=True)
    # remains.drop(
    #     axis=1, labels=["Unnamed: 1", "Unnamed: 2", "Unnamed: 4"], inplace=True
    # )
    # remains.drop(axis=0, labels=remains.tail(1).index, inplace=True)
    # remains_col_name = [
    #     "line_of_business", "warehouse", "parent_element", "nomenclature", "party_sign",
    #     "buying_season", "nomenclature_series", "mtn", "origin_country", "germination",
    #     "crop_year", "quantity_per_pallet", "active_substance", "certificate",
    #     "certificate_start_date", "certificate_end_date", "buh", "skl", "weight", "storage",
    # ]
    # remains.columns = remains_col_name
    # remains["buh"]=remains["buh"].fillna(0)
    # remains["skl"]=remains["skl"].fillna(0)
    # remains.fillna("", inplace=True)
    # remains["product"] = remains.apply(
    #     lambda row: str(row["nomenclature"]).rstrip()
    #                 + " "
    #                 + str(row["party_sign"]).rstrip()
    #                 + " "
    #                 + str(row["buying_season"]).rstrip(),
    #     axis=1,
    # )
    # remains = remains.loc[remains["line_of_business"].isin(valid_line_of_business)]
    # remains = remains.loc[remains["warehouse"].isin(valid_warehouse)]
    # return remains


def process_payment(content: bytes) -> pd.DataFrame:
    payment = read_excel_content(content)
    # Удаляем ненужные строки и колонки
    payment.drop(axis=0, labels=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9], inplace=True)
    payment.drop(
        axis=1, labels=["Unnamed: 1", "Unnamed: 2", "Unnamed: 7"], inplace=True
    )
    payment.drop(axis=0, labels=payment.tail(1).index, inplace=True)

    # Новые имена колонок
    payment_col_name = [
        "contract_supplement",
        "contract_type",
        "prepayment_amount",
        "amount_of_credit",
        "prepayment_percentage",
        "loan_percentage",
        "planned_amount",
        "planned_amount_excluding_vat",
        "actual_sale_amount",
        "actual_payment_amount",
    ]
    payment.columns = payment_col_name

    # Приведение к нужным типам
    numeric_columns = [
        "prepayment_amount",
        "amount_of_credit",
        "prepayment_percentage",
        "loan_percentage",
        "planned_amount",
        "planned_amount_excluding_vat",
        "actual_sale_amount",
        "actual_payment_amount",
    ]

    for col in numeric_columns:
        payment[col] = pd.to_numeric(payment[col], errors="coerce").fillna(0)

    # contract_supplement и contract_type — текстовые
    payment["contract_supplement"] = (
        payment["contract_supplement"].astype(str).fillna("")
    )
    payment["contract_type"] = payment["contract_type"].astype(str).fillna("")

    return payment
    # payment.drop(axis=0, labels=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9], inplace=True)
    # payment.drop(
    #     axis=1, labels=["Unnamed: 1", "Unnamed: 2", "Unnamed: 7"], inplace=True
    # )
    # payment.drop(axis=0, labels=payment.tail(1).index, inplace=True)
    # payment_col_name = [
    #     "contract_supplement", "contract_type", "prepayment_amount",
    #     "amount_of_credit", "prepayment_percentage", "loan_percentage",
    #     "planned_amount", "planned_amount_excluding_vat", "actual_sale_amount",
    #     "actual_payment_amount",
    # ]
    # payment.columns = payment_col_name
    # payment.fillna(0, inplace=True)
    # return payment


def process_moved_data(content: bytes) -> pd.DataFrame:
    moved = read_excel_content(content, sheet_name="Данные")
    # Задаём имена колонок
    moved_col_names = [
        "order",
        "date",
        "line_of_business",
        "product",
        "qt_order",
        "qt_moved",
        "party_sign",
        "period",
        "contract",
    ]
    moved.columns = moved_col_names

    # Приводим числовые колонки
    for col in ["qt_order", "qt_moved"]:
        moved[col] = pd.to_numeric(moved[col], errors="coerce").fillna(0)

    # Остальные колонки - текстовые
    text_columns = [
        "order",
        "date",
        "line_of_business",
        "product",
        "party_sign",
        "period",
        "contract",
    ]
    for col in text_columns:
        moved[col] = moved[col].astype(str).fillna("")

    # Убираем лишние строки, если есть (по аналогии с другими файлами)
    moved = moved.dropna(how="all")  # Удаляет полностью пустые строки
    moved = moved.reset_index(drop=True)

    return moved
    # moved_col_names = [
    #     "order", "date", "line_of_business", "product", "qt_order",
    #     "qt_moved", "party_sign", "period", "contract",
    # ]
    # moved.columns = moved_col_names
    # return moved


# Асинхронная функция для отправки сообщений менеджерам
async def send_message_to_managers():
    """Отправляет сообщение всем менеджерам в Telegram."""
    user_tg_id = MANAGERS_ID.values()
    now = datetime.now() + timedelta(
        hours=3
    )  # Убедитесь, что это правильное смещение часового пояса
    time_format = "%d-%m-%Y %H:%M:%S"
    message_text = (
        f"Дані в боті оновлені.{chr(10)}І вони актуальні станом на… {now:{time_format}}"
    )

    for i in user_tg_id:
        try:
            await bot.send_message(chat_id=i, text=message_text)
            print(f"Sent message to manager ID: {i}")
        except Exception as e:
            print(f"Failed to send message to manager ID {i}: {e}")


# Синхронная функция для обработки и сохранения данных в базу данных
# Она будет запускаться в отдельном потоке (executor)
def save_processed_data_to_db_sync(
    av_stock_content: bytes,
    remains_content: bytes,
    submissions_content: bytes,
    payment_content: bytes,
    moved_content: bytes,
):
    """
    Синхронная функция для обработки и сохранения данных в базу данных.
    Выполняется в отдельном потоке, чтобы не блокировать FastAPI.
    """
    try:
        # 1. Обработка файлов Pandas
        av_stock = process_av_stock(av_stock_content)
        remains = process_remains_reg(remains_content)
        submissions = process_submissions(submissions_content)
        payment = process_payment(payment_content)
        moved = process_moved_data(moved_content)

        print("Pandas processing complete.")

        # 2. Подготовка и сохранение данных через Piccolo ORM
        # --- PRODUCT GUIDE ---
        av_stock_tmp = av_stock[
            ["product", "line_of_business", "active_substance"]
        ].copy()
        remains_tmp = remains[
            ["product", "line_of_business", "active_substance"]
        ].copy()
        submissions_tmp = (
            submissions[["product", "line_of_business", "active_ingredient"]]
            .copy()
            .rename(columns={"active_ingredient": "active_substance"})
        )

        pr = pd.concat([av_stock_tmp, submissions_tmp, remains_tmp], ignore_index=True)
        pr["product"] = pr["product"].astype(str).str.rstrip()
        product_guide = pr.drop_duplicates(["product"]).reset_index(drop=True)

        # Выполняем асинхронные операции Piccolo ORM с помощью asyncio.run()
        # Это безопасно, так как мы находимся в отдельном потоке
        asyncio.run(
            ProductGuide.delete(force=True).run()
        )  # Очищаем таблицу перед вставкой
        product_guide["id"] = product_guide.apply(
            lambda _: uuid.uuid4(), axis=1
        )  # Генерируем UUID
        product_guide_dicts = product_guide[
            ["id", "product", "line_of_business", "active_substance"]
        ].to_dict(orient="records")
        asyncio.run(
            ProductGuide.insert(*[ProductGuide(**d) for d in product_guide_dicts]).run()
        )
        print(f"ProductGuide inserted: {len(product_guide_dicts)} records.")

        # --- REMAINS ---
        remains["product"] = remains["product"].astype(str).str.rstrip()
        remains_sql = pd.merge(
            remains, product_guide, on="product", suffixes=("", "_guide")
        )
        remains_sql = remains_sql[
            [
                "line_of_business",
                "warehouse",
                "parent_element",
                "nomenclature",
                "party_sign",
                "buying_season",
                "nomenclature_series",
                "mtn",
                "origin_country",
                "germination",
                "crop_year",
                "quantity_per_pallet",
                "active_substance",
                "certificate",
                "certificate_start_date",
                "certificate_end_date",
                "buh",
                "skl",
                "weight",
                "id",
            ]
        ].copy()
        remains_sql.rename(columns={"id": "product"}, inplace=True)
        remains_sql["weight"] = remains_sql["weight"].astype(str)
        remains_sql["quantity_per_pallet"] = remains_sql["quantity_per_pallet"].astype(
            str
        )
        remains_sql.insert(0, "id", remains_sql.apply(lambda _: uuid.uuid4(), axis=1))

        asyncio.run(Remains.delete(force=True).run())
        remains_dicts = remains_sql.to_dict(orient="records")
        asyncio.run(Remains.insert(*[Remains(**d) for d in remains_dicts]).run())
        print(f"Remains inserted: {len(remains_dicts)} records.")

        # # --- AVAILABLE STOCK ---
        # av_stock["product"] = av_stock["product"].astype(str).str.rstrip()
        # available_stock_sql = pd.merge(av_stock, product_guide, on="product", suffixes=("", "_guide"))
        # available_stock_sql = available_stock_sql[[
        #     "nomenclature", "party_sign", "buying_season", "division",
        #     "line_of_business", "available", "id"
        # ]].copy()
        # available_stock_sql.rename(columns={"id": "product"}, inplace=True)
        # available_stock_sql['available'] = pd.to_numeric(available_stock_sql['available'], errors='coerce').fillna(0)
        # available_stock_sql.insert(0, "id", available_stock_sql.apply(lambda _: uuid.uuid4(), axis=1))
        #
        # asyncio.run(AvailableStock.delete(force=True).run())
        # available_stock_dicts = available_stock_sql.to_dict(orient='records')
        # asyncio.run(AvailableStock.insert(*[AvailableStock(**d) for d in available_stock_dicts]).run())
        # print(f"AvailableStock inserted: {len(available_stock_dicts)} records.")
        # --- AVAILABLE STOCK ---
        av_stock["product"] = av_stock["product"].astype(str).str.rstrip()
        available_stock_sql = pd.merge(
            av_stock, product_guide, on="product", suffixes=("", "_guide")
        )
        available_stock_sql = available_stock_sql[
            [
                "nomenclature",
                "party_sign",
                "buying_season",
                "division",
                "line_of_business",
                "available",
                "id",
            ]
        ].copy()
        available_stock_sql.rename(columns={"id": "product"}, inplace=True)
        available_stock_sql["available"] = pd.to_numeric(
            available_stock_sql["available"], errors="coerce"
        ).fillna(0)
        available_stock_sql.insert(
            0, "id", available_stock_sql.apply(lambda _: uuid.uuid4(), axis=1)
        )

        asyncio.run(AvailableStock.delete(force=True).run())

        available_stock_dicts = available_stock_sql.to_dict(orient="records")

        # --- Разбивка на чанки ---
        CHUNK_SIZE = 1000  # Подбери размер чанка под свою задачу

        for i in range(0, len(available_stock_dicts), CHUNK_SIZE):
            chunk = available_stock_dicts[i : i + CHUNK_SIZE]
            asyncio.run(
                AvailableStock.insert(*[AvailableStock(**d) for d in chunk]).run()
            )
            print(f"Inserted chunk {i // CHUNK_SIZE + 1} with {len(chunk)} records.")

        print(f"AvailableStock inserted: {len(available_stock_dicts)} records.")

        # # --- SUBMISSIONS ---
        # submissions['product'] = submissions['product'].astype(str).str.rstrip()
        # submissions_sql = pd.merge(submissions, product_guide, on="product", suffixes=("", "_guide"))
        # submissions_sql = submissions_sql[[
        #     "division", "manager", "company_group", "client", "contract_supplement",
        #     "parent_element", "manufacturer", "active_ingredient", "nomenclature",
        #     "party_sign", "buying_season", "line_of_business", "period",
        #     "shipping_warehouse", "document_status", "delivery_status",
        #     "shipping_address", "transport", "plan", "fact", "different",
        #     "id"
        # ]].copy()
        # submissions_sql.rename(columns={"id": "product"}, inplace=True)
        # submissions_sql.insert(0, "id", submissions_sql.apply(lambda _: uuid.uuid4(), axis=1))
        #
        # submissions_sql['plan'] = pd.to_numeric(submissions_sql['plan'], errors='coerce').fillna(0)
        # submissions_sql['fact'] = pd.to_numeric(submissions_sql['fact'], errors='coerce').fillna(0)
        # submissions_sql['different'] = pd.to_numeric(submissions_sql['different'], errors='coerce').fillna(0)
        #
        # asyncio.run(Submissions.delete(force=True).run())
        # submissions_dicts = submissions_sql.to_dict(orient='records')
        # asyncio.run(Submissions.insert(*[Submissions(**d) for d in submissions_dicts]).run())
        # print(f"Submissions inserted: {len(submissions_dicts)} records.")
        # --- SUBMISSIONS ---
        submissions["product"] = submissions["product"].astype(str).str.rstrip()
        submissions_sql = pd.merge(
            submissions, product_guide, on="product", suffixes=("", "_guide")
        )
        submissions_sql = submissions_sql[
            [
                "division",
                "manager",
                "company_group",
                "client",
                "contract_supplement",
                "parent_element",
                "manufacturer",
                "active_ingredient",
                "nomenclature",
                "party_sign",
                "buying_season",
                "line_of_business",
                "period",
                "shipping_warehouse",
                "document_status",
                "delivery_status",
                "shipping_address",
                "transport",
                "plan",
                "fact",
                "different",
                "id",
            ]
        ].copy()
        submissions_sql.rename(columns={"id": "product"}, inplace=True)
        submissions_sql.insert(
            0, "id", submissions_sql.apply(lambda _: uuid.uuid4(), axis=1)
        )
        # Приводим всё к строке, кроме числовых
        for col in submissions_sql.columns:
            if col not in ["plan", "fact", "different"]:
                submissions_sql[col] = submissions_sql[col].astype(str)

        submissions_sql["plan"] = pd.to_numeric(
            submissions_sql["plan"], errors="coerce"
        ).fillna(0)
        submissions_sql["fact"] = pd.to_numeric(
            submissions_sql["fact"], errors="coerce"
        ).fillna(0)
        submissions_sql["different"] = pd.to_numeric(
            submissions_sql["different"], errors="coerce"
        ).fillna(0)

        asyncio.run(Submissions.delete(force=True).run())
        submissions_dicts = submissions_sql.to_dict(orient="records")

        # --- Вставка чанками ---
        chunk_size = 1000  # Можешь подкорректировать, например, 500 или 2000
        total_chunks = math.ceil(len(submissions_dicts) / chunk_size)

        for i in range(total_chunks):
            chunk = submissions_dicts[i * chunk_size : (i + 1) * chunk_size]
            asyncio.run(Submissions.insert(*[Submissions(**d) for d in chunk]).run())
            print(f"Inserted chunk {i + 1}/{total_chunks} ({len(chunk)} records)")

        print(f"Submissions inserted: {len(submissions_dicts)} records.")
        # --- PAYMENT ---
        for col in [
            "prepayment_amount",
            "amount_of_credit",
            "prepayment_percentage",
            "loan_percentage",
            "planned_amount",
            "planned_amount_excluding_vat",
            "actual_sale_amount",
            "actual_payment_amount",
        ]:
            payment[col] = pd.to_numeric(payment[col], errors="coerce").fillna(0)

        payment.insert(0, "id", payment.apply(lambda _: uuid.uuid4(), axis=1))

        asyncio.run(Payment.delete(force=True).run())
        payment_dicts = payment.to_dict(orient="records")
        asyncio.run(Payment.insert(*[Payment(**d) for d in payment_dicts]).run())
        print(f"Payment inserted: {len(payment_dicts)} records.")

        # # --- MOVED DATA ---
        # moved['qt_order'] = pd.to_numeric(moved['qt_order'], errors='coerce').fillna(0)
        # moved['qt_moved'] = pd.to_numeric(moved['qt_moved'], errors='coerce').fillna(0)
        #
        # moved.insert(0, "id", moved.apply(lambda _: uuid.uuid4(), axis=1))
        #
        # asyncio.run(MovedData.delete(force=True).run())
        # moved_dicts = moved.to_dict(orient='records')
        # asyncio.run(MovedData.insert(*[MovedData(**d) for d in moved_dicts]).run())
        # print(f"MovedData inserted: {len(moved_dicts)} records.")
        # # --- MOVED DATA ---
        # moved['qt_order'] = pd.to_numeric(moved['qt_order'],
        #                                   errors='coerce').fillna(0)
        # moved['qt_moved'] = pd.to_numeric(moved['qt_moved'],
        #                                   errors='coerce').fillna(0)
        #
        # moved.insert(0, "id", moved.apply(lambda _: uuid.uuid4(), axis=1))
        #
        # asyncio.run(MovedData.delete(force=True).run())
        # moved_dicts = moved.to_dict(orient='records')
        #
        # # --- Вставка чанками ---
        # chunk_size = 1000  # Можешь изменить, если нужно
        # total_chunks = math.ceil(len(moved_dicts) / chunk_size)
        #
        # for i in range(total_chunks):
        #     chunk = moved_dicts[i * chunk_size: (i + 1) * chunk_size]
        #     asyncio.run(
        #         MovedData.insert(*[MovedData(**d) for d in chunk]).run())
        #     print(
        #         f"Inserted chunk {i + 1}/{total_chunks} ({len(chunk)} records)")
        #
        # print(f"MovedData inserted: {len(moved_dicts)} records.")
        # --- MOVED DATA ---

        # Преобразование даты
        moved["date"] = pd.to_datetime(moved["date"], errors="coerce").dt.date

        # Преобразование всех остальных колонок, кроме 'date', в строки
        for col in moved.columns:
            if col != "date":
                moved[col] = moved[col].astype(str).str.strip()

        # Вставляем id
        moved.insert(0, "id", moved.apply(lambda _: uuid.uuid4(), axis=1))

        # Очистка таблицы и вставка чанками
        asyncio.run(MovedData.delete(force=True).run())
        moved_dicts = moved.to_dict(orient="records")

        # --- Вставка чанками ---
        chunk_size = 1000  # Можешь изменить, если нужно
        total_chunks = math.ceil(len(moved_dicts) / chunk_size)

        for i in range(total_chunks):
            chunk = moved_dicts[i * chunk_size : (i + 1) * chunk_size]
            asyncio.run(MovedData.insert(*[MovedData(**d) for d in chunk]).run())
            print(f"Inserted chunk {i + 1}/{total_chunks} ({len(chunk)} records)")

        print(f"MovedData inserted: {len(moved_dicts)} records.")

        # Отправка уведомления после успешной загрузки всех данных
        asyncio.run(send_message_to_managers())

        return {
            "status": "success",
            "message": "All data processed and saved successfully.",
        }

    except Exception as e:
        print(f"Error in save_processed_data_to_db_sync: {e}")
        return {"status": "error", "message": f"Failed to process data: {str(e)}"}


# Эндпоинт FastAPI для загрузки всех файлов
@app.post("/upload_all_data/", summary="Upload all Excel files and process data")
async def upload_all_data(
    background_tasks: BackgroundTasks,  # FastAPI будет управлять фоновыми задачами
    submissions_file: UploadFile = File(
        ..., description="Excel file for Submissions (Заявки.xlsx)"
    ),
    av_stock_file: UploadFile = File(
        ...,
        description="Excel file for Available Stock (Доступность товара подразделения.xlsx)",
    ),
    remains_file: UploadFile = File(
        ..., description="Excel file for Remains (Остатки.xlsx)"
    ),
    payment_file: UploadFile = File(
        ..., description="Excel file for Payment (оплата.xlsx)"
    ),
    moved_data_file: UploadFile = File(
        ..., description="Excel file for Moved Data (Заказано_Перемещено.xlsx)"
    ),
    user: dict = Depends(get_current_user),  # Проверка аутентификации пользователя
):
    """
    Принимает несколько Excel-файлов, обрабатывает их с помощью Pandas,
    и сохраняет данные в базу данных PostgreSQL.
    Эта операция выполняется в фоновом потоке, чтобы не блокировать API.
    """
    files = {
        "submissions": submissions_file,
        "av_stock": av_stock_file,
        "remains": remains_file,
        "payment": payment_file,
        "moved_data": moved_data_file,
    }

    # Проверяем расширения файлов
    for name, file in files.items():
        if not file.filename.endswith((".xlsx", ".xls")):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid file type for {name}. Only .xlsx or .xls files are allowed.",
            )

    try:
        # Читаем содержимое файлов в байты
        submissions_content = await submissions_file.read()
        av_stock_content = await av_stock_file.read()
        remains_content = await remains_file.read()
        payment_content = await payment_file.read()
        moved_data_content = await moved_data_file.read()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to read file contents: {e}",
        )

    # Запускаем синхронную функцию обработки данных в отдельном потоке,
    # используя ThreadPoolExecutor.
    background_tasks.add_task(
        executor.submit,  # Используем executor.submit для запуска синхронной функции в отдельном потоке
        save_processed_data_to_db_sync,
        av_stock_content,
        remains_content,
        submissions_content,
        payment_content,
        moved_data_content,
    )

    return JSONResponse(
        content={
            "message": "Data processing started in the background. You will be notified by Telegram when complete."
        },
        status_code=status.HTTP_202_ACCEPTED,
    )


# Пример эндпоинта для получения данных из ProductGuide
# @app.get("/product_guide/{product_name}", summary="Get ProductGuide details by product name")
# async def get_product_guide(product_name: str):
#     product_name_cleaned = product_name.strip()
#     # Ищем продукт по имени
#     product = await ProductGuide.objects().where(ProductGuide.product == product_name_cleaned).first()
#     if not product:
#         raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Product not found in guide")
#     return product.to_dict()
@app.get("/remains")
async def get_remains():
    remains = await Remains.select()
    return remains


@app.get("/remains/{id}")
async def get_remains_by_product(id):
    remains = await Remains.select().where(Remains.product == id)
    return remains


@app.get("/products")
async def get_products(query: Optional[str] = Query(None)):
    if query:
        products = await ProductGuide.select().where(
            ProductGuide.product.ilike(f"%{query}%")
        )
    else:
        products = await ProductGuide.select()
    return products


# @app.get("/products/{query}")
# async def get_products_by_query(query):
#     products = await ProductGuide.select().where(
#         ProductGuide.product.ilike(f"%{query}%")
#     )
#     return products


@app.get("/product/{id}")
async def get_product_by_id(id):
    product = await ProductGuide.select().where(ProductGuide.id == id)
    return product


if __name__ == "__main__":
    uvicorn.run(
        "new_agri_bot_backend.main:app", host="127.0.0.1", port=8000, reload=True
    )
