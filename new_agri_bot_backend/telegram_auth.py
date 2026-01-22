# app/telegram_auth.py
import hmac, hashlib, json
from urllib.parse import parse_qsl
from datetime import datetime, timezone, timedelta
from typing import Optional

from fastapi import HTTPException, status, APIRouter, Header, Depends
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel
from dotenv import load_dotenv
import os
from jose import JWTError, jwt

from .tables import Users  # Импорт вашей модели Users
from .config import (
    TELEGRAM_BOT_TOKEN,
)  # Убедитесь, что TELEGRAM_BOT_TOKEN есть в config.py

load_dotenv()

# Конфигурация JWT
SECRET_KEY = os.getenv(
    "SECRET_KEY", "your-secret-key-change-it-in-production"
)  # Замените на надежный ключ
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24 * 7  # Токен валиден 7 дней

router = APIRouter(tags=["Аутентифікація"])
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="auth/login-widget", auto_error=False)


class InitDataModel(BaseModel):
    initData: str


class TelegramLoginWidgetData(BaseModel):
    id: int
    first_name: str
    last_name: Optional[str] = None
    username: Optional[str] = None
    photo_url: Optional[str] = None
    auth_date: int
    hash: str


class Token(BaseModel):
    access_token: str
    token_type: str


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


def check_telegram_auth(init_data: str) -> dict:
    """
    Проверяет init_data, полученные от Telegram Mini App,
    строго следуя предоставленному алгоритму двухэтапного HMAC-SHA256.
    """
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError(
            "TELEGRAM_BOT_TOKEN не установлен. Проверьте config.py или переменные окружения."
        )

    parsed = dict(parse_qsl(init_data))
    hash_ = parsed.pop("hash", None)
    if not hash_:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Отсутствует 'hash' в данных инициализации Telegram.",
        )

    sorted_items = sorted(parsed.items())
    data_check_string_parts = []
    for k, v in sorted_items:
        part = f"{k}={v}"
        data_check_string_parts.append(part)

    data_check_string = "\n".join(data_check_string_parts)

    secret_key_intermediate = hmac.new(
        key=b"WebAppData",
        msg=TELEGRAM_BOT_TOKEN.encode("utf-8"),
        digestmod=hashlib.sha256,
    ).digest()

    calculated_hash = hmac.new(
        key=secret_key_intermediate,
        msg=data_check_string.encode("utf-8"),
        digestmod=hashlib.sha256,
    ).hexdigest()

    if calculated_hash != hash_:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Неверный хэш данных инициализации Telegram. Хэши не совпадают.",
        )
    return parsed


def check_telegram_login_widget(data: dict) -> bool:
    """
    Проверяет данные от Telegram Login Widget.
    Алгоритм отличается от Web App (initData).
    """
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN не установлен.")

    received_hash = data.pop("hash", None)
    if not received_hash:
        return False

    # 1. Сортируем и собираем строку data-check-string
    data_check_arr = []
    for key, value in sorted(data.items()):
        if value is not None:  # Важно: пропускаем None
            data_check_arr.append(f"{key}={value}")
    data_check_string = "\n".join(data_check_arr)

    # 2. Вычисляем секретный ключ: SHA256 от токена бота
    secret_key = hashlib.sha256(TELEGRAM_BOT_TOKEN.encode("utf-8")).digest()

    # 3. Вычисляем HMAC-SHA256
    calculated_hash = hmac.new(
        key=secret_key,
        msg=data_check_string.encode("utf-8"),
        digestmod=hashlib.sha256,
    ).hexdigest()

    # 4. Сравниваем
    if calculated_hash != received_hash:
        return False

    # 5. Проверка времени (опционально, но рекомендуется)
    # if (datetime.now().timestamp() - data["auth_date"]) > 86400:
    #     return False

    return True


@router.post("/auth", summary="Аутентификация пользователя Telegram Mini App")
async def auth(data: InitDataModel):
    """
    Эндпоинт для аутентификации пользователей Telegram Mini App.
    """
    print(
        f"[{datetime.now(timezone.utc)}] Получены RAW INIT DATA: {data.initData[:100]}..."
    )

    try:
        parsed_init_data = check_telegram_auth(data.initData)
    except HTTPException as e:
        print(
            f"[{datetime.now(timezone.utc)}] Ошибка аутентификации initData: {e.detail}"
        )
        raise e
    except Exception as e:
        print(
            f"[{datetime.now(timezone.utc)}] Неожиданная ошибка при проверке initData: {e}"
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Внутренняя ошибка сервера при проверке данных: {e}",
        )

    user_info_str = parsed_init_data.get("user")
    if not user_info_str:
        print(f"[{datetime.now(timezone.utc)}] Отсутствует поле 'user' в initData.")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="В данных инициализации Telegram отсутствует информация о пользователе (поле 'user').",
        )

    try:
        user_data = json.loads(user_info_str)
    except json.JSONDecodeError:
        print(
            f"[{datetime.now(timezone.utc)}] Неверный формат JSON для данных пользователя: {user_info_str[:50]}..."
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Неверный формат JSON для данных пользователя в initData.",
        )

    telegram_id = user_data.get("id")
    if not isinstance(telegram_id, int):
        print(
            f"[{datetime.now(datetime.timezone.utc)}] Telegram ID пользователя не является целым числом: {telegram_id}."
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Telegram ID пользователя не является целым числом или отсутствует.",
        )

    current_utc_time = datetime.now(timezone.utc)
    user_in_db = (
        await Users.objects().where(Users.telegram_id == telegram_id).first().run()
    )

    if not user_in_db:
        print(
            f"[{current_utc_time}] Пользователь с Telegram ID {telegram_id} НЕ НАЙДЕН в БД. Доступ запрещен."
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Доступ запрещен. Пользователь с Telegram ID {telegram_id} не зарегистрирован в системе.",
        )
    if not user_in_db.is_allowed:
        print(
            f"[{current_utc_time}] Пользователь {user_in_db.username or user_in_db.telegram_id} (ID: {user_in_db.telegram_id}) найден, но is_allowed=False. Доступ запрещен."
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Доступ запрещен. Ваш аккаунт (Telegram ID {telegram_id}) не имеет активного разрешения. Обратитесь к администратору.",
        )

    print(
        f"[{current_utc_time}] Пользователь с Telegram ID {telegram_id} найден. Обновляем данные."
    )
    user_in_db.username = user_data.get("username")
    user_in_db.first_name = user_data.get("first_name")
    user_in_db.last_name = user_data.get("last_name")
    user_in_db.last_activity_date = current_utc_time
    await user_in_db.save().run()
    message = "Данные пользователя успешно обновлены."
    print(
        f"[{current_utc_time}] Данные пользователя {user_in_db.username or user_in_db.telegram_id} (ID: {user_in_db.telegram_id}) обновлены."
    )

    return {
        "message": message,
        "telegram_id": user_in_db.telegram_id,
        "username": user_in_db.username,
        "first_name": user_in_db.first_name,
        "last_name": user_in_db.last_name,
        "is_allowed": user_in_db.is_allowed,
        "is_admin": user_in_db.is_admin,
        "full_name_for_orders": user_in_db.full_name_for_orders,
    }


@router.post("/auth/login-widget", summary="Аутентификация через Telegram Login Widget")
async def auth_login_widget(data: TelegramLoginWidgetData):
    """
    Эндпоинт для входа через виджет Telegram на сайте.
    Возвращает JWT токен для доступа к API.
    """
    # 1. Преобразуем Pydantic модель в dict, исключая None
    data_dict = data.dict(exclude_none=True)

    # 2. Проверяем подпись
    if not check_telegram_login_widget(data_dict.copy()):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid Telegram Widget data signature",
        )

    # 3. Ищем пользователя в БД
    user = await Users.objects().where(Users.telegram_id == data.id).first().run()

    if not user:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User not found in database",
        )

    if not user.is_allowed:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Access denied"
        )

    # 4. Генерируем JWT токен
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": str(user.telegram_id)}, expires_delta=access_token_expires
    )

    return {
        "status": "ok",
        "access_token": access_token,
        "token_type": "bearer",
        "user": {
            "id": user.telegram_id,
            "username": user.username,
            "first_name": user.first_name,
            "is_admin": user.is_admin,
        },
    }


@router.get("/get_user")
async def get_current_telegram_user(
    x_telegram_init_data: str = Header(
        ...,
        alias="X-Telegram-Init-Data",
        description="Дані ініціалізації Telegram Mini App",
    )
):
    """
    Залежність, яка перевіряє Telegram initData з хедера запиту.
    """
    if not x_telegram_init_data:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Відсутній хедер 'X-Telegram-Init-Data'.",
        )

    try:
        parsed_init_data = check_telegram_auth(x_telegram_init_data)
    except HTTPException as e:
        raise e
    except Exception as e:
        print(
            f"[{datetime.now(timezone.utc)}] Несподівана помилка при перевірці initData в залежності: {e}"
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Внутрішня помилка сервера під час перевірки даних аутентифікації: {e}",
        )

    user_info_str = parsed_init_data.get("user")
    if not user_info_str:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Недостатньо даних користувача в initData.",
        )

    try:
        user_data = json.loads(user_info_str)
    except json.JSONDecodeError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Невірний формат даних користувача в initData.",
        )

    telegram_id = user_data.get("id")
    if not isinstance(telegram_id, int):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Невірний формат Telegram ID користувача.",
        )

    user_in_db = (
        await Users.objects().where(Users.telegram_id == telegram_id).first().run()
    )

    if not user_in_db or not user_in_db.is_allowed:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Доступ заборонено. Користувач не зареєстрований або не має дозволу.",
        )

    return user_in_db


async def get_current_user_jwt(token: str = Depends(oauth2_scheme)):
    """
    Зависимость для получения пользователя из JWT токена.
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        # sub в JWT обычно строка, но у нас ID - int. Приводим к int.
        telegram_id_str = payload.get("sub")
        if telegram_id_str is None:
            raise credentials_exception
        telegram_id = int(telegram_id_str)
    except (JWTError, ValueError):
        raise credentials_exception

    user = await Users.objects().where(Users.telegram_id == telegram_id).first().run()
    if user is None:
        raise credentials_exception
    if not user.is_allowed:
        raise HTTPException(status_code=403, detail="Access denied")
    return user


async def get_current_user_universal(
    x_telegram_init_data: Optional[str] = Header(
        None, alias="X-Telegram-Init-Data"
    ),
    token: Optional[str] = Depends(oauth2_scheme),
):
    """
    Универсальная зависимость:
    1. Пробует аутентифицировать через JWT (Bearer Token).
    2. Если токена нет, пробует через X-Telegram-Init-Data.
    """
    # 1. Попытка через JWT
    if token:
        try:
            return await get_current_user_jwt(token)
        except HTTPException:
            # Если токен невалиден, но есть initData, попробуем её.
            # Если initData нет, то ошибка JWT будет финальной.
            if not x_telegram_init_data:
                raise

    # 2. Попытка через InitData
    if x_telegram_init_data:
        return await get_current_telegram_user(x_telegram_init_data)

    # 3. Если ничего нет
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Not authenticated. Provide either 'Authorization: Bearer ...' or 'X-Telegram-Init-Data' header.",
    )
