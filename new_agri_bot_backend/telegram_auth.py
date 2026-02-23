# app/telegram_auth.py
import hmac, hashlib, json, uuid
from urllib.parse import parse_qsl
from datetime import datetime, timezone, timedelta

from fastapi import HTTPException, status, APIRouter, Header, Depends
from pydantic import BaseModel
from dotenv import load_dotenv
import os

from .tables import Users  # Импорт вашей модели Users
from .config import (
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_WIDGET_BOT_TOKEN,
)  # TELEGRAM_WIDGET_BOT_TOKEN — токен бота без Mini App для Login Widget

load_dotenv()

# Если TELEGRAM_BOT_TOKEN также загружается здесь, убедитесь, что он один источник истины,
# либо просто импортируйте из config.py
# TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN") # Лучше брать из config.py

router = APIRouter(tags=["Аутентифікація"])


class InitDataModel(BaseModel):
    initData: str


class TelegramWidgetData(BaseModel):
    """Дані від Telegram Login Widget (onTelegramAuth callback)"""
    id: int
    first_name: str
    last_name: str | None = None
    username: str | None = None
    photo_url: str | None = None
    auth_date: int
    hash: str


def check_widget_auth(data: TelegramWidgetData) -> None:
    """
    Верифікує дані від Telegram Login Widget.
    Алгоритм відрізняється від Mini App:
      secret_key = SHA256(bot_token)  # не HMAC("WebAppData", token)
      data_check_string = відсортовані поля (без hash), з'єднані \\n
      hash = HMAC-SHA256(data_check_string, secret_key)
    """
    if not TELEGRAM_WIDGET_BOT_TOKEN:
        raise RuntimeError("TELEGRAM_WIDGET_BOT_TOKEN не встановлено.")

    # Формуємо словник полів (без hash)
    fields = {
        "auth_date": str(data.auth_date),
        "first_name": data.first_name,
        "id": str(data.id),
    }
    if data.last_name:
        fields["last_name"] = data.last_name
    if data.username:
        fields["username"] = data.username
    if data.photo_url:
        fields["photo_url"] = data.photo_url

    # Сортуємо та формуємо data_check_string
    data_check_string = "\n".join(
        f"{k}={v}" for k, v in sorted(fields.items())
    )

    # secret_key = SHA256(widget_bot_token) — використовуємо окремий бот без Mini App
    secret_key = hashlib.sha256(TELEGRAM_WIDGET_BOT_TOKEN.encode("utf-8")).digest()

    calculated_hash = hmac.new(
        key=secret_key,
        msg=data_check_string.encode("utf-8"),
        digestmod=hashlib.sha256,
    ).hexdigest()

    if calculated_hash != data.hash:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Невірний хеш даних Telegram Widget.",
        )

    # Перевіряємо свіжість даних (не старіше 24 годин)
    age = datetime.now(timezone.utc).timestamp() - data.auth_date
    if age > 86400:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Дані авторизації застаріли (більше 24 годин).",
        )


def check_telegram_auth(init_data: str) -> dict:
    """
    Проверяет init_data, полученные от Telegram Mini App,
    строго следуя предоставленному алгоритму двухэтапного HMAC-SHA256.
    """
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError(
            "TELEGRAM_BOT_TOKEN не установлен. Проверьте config.py или переменные окружения."
        )

    # print("\n--- НАЧАЛО ДЕТАЛЬНОЙ ОТЛАДКИ check_telegram_auth ---")
    # print(f"1. Получена init_data: '{init_data}'")
    # print(f"2. Длина init_data: {len(init_data)}")
    # print(
    #     f"3. TELEGRAM_BOT_TOKEN (обрезан): '{TELEGRAM_BOT_TOKEN[:5]}...{TELEGRAM_BOT_TOKEN[-5:]}'"
    # )
    # print(f"4. Длина TELEGRAM_BOT_TOKEN: {len(TELEGRAM_BOT_TOKEN)}")

    parsed = dict(parse_qsl(init_data))
    hash_ = parsed.pop("hash", None)
    if not hash_:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Отсутствует 'hash' в данных инициализации Telegram.",
        )

    # print(f"5. Извлечен 'hash_': '{hash_}'")
    # print(f"6. Распарсенные данные (после удаления 'hash'): {parsed}")
    # print(f"7. Количество полей для проверки: {len(parsed)}")

    sorted_items = sorted(parsed.items())
    data_check_string_parts = []
    for k, v in sorted_items:
        part = f"{k}={v}"
        data_check_string_parts.append(part)

    data_check_string = "\n".join(data_check_string_parts)

    # print(f"8. Сформированная data_check_string (длина {len(data_check_string)}):")
    # print("--- НАЧАЛО data_check_string ---")
    # print(data_check_string)
    # print("--- КОНЕЦ data_check_string ---")

    secret_key_intermediate = hmac.new(
        key=b"WebAppData",
        msg=TELEGRAM_BOT_TOKEN.encode("utf-8"),
        digestmod=hashlib.sha256,
    ).digest()

    # print(
    #     f"9. Промежуточный секретный ключ (HMAC(WebAppData, bot_token)) (hex): {secret_key_intermediate.hex()}"
    # )

    calculated_hash = hmac.new(
        key=secret_key_intermediate,
        msg=data_check_string.encode("utf-8"),
        digestmod=hashlib.sha256,
    ).hexdigest()

    # print(f"10. Вычисленный финальный хэш: '{calculated_hash}'")
    # print(f"11. Хэши совпадают? (Calculated == Received) : {calculated_hash == hash_}")
    # print("--- КОНЕЦ ДЕТАЛЬНОЙ ОТЛАДКИ check_telegram_auth ---\n")

    if calculated_hash != hash_:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Неверный хэш данных инициализации Telegram. Хэши не совпадают.",
        )
    return parsed


@router.post("/auth", summary="Аутентификация пользователя Telegram Mini App")
async def auth(data: InitDataModel):
    """
    Эндпоинт для аутентификации пользователей Telegram Mini App.

    1. Проверяет подлинность данных `initData` от Telegram.
    2. Извлекает `telegram_id` и другие данные пользователя.
    3. Находит пользователя в базе данных `Users` по `telegram_id`.
       - Если пользователь не найден: возвращает ошибку 403 Forbidden, так как доступ ему закрыт.
       - Если пользователь найден: обновляет его `username`, `first_name`, `last_name`
         и `last_activity_date`.
    4. Возвращает основные данные пользователя из вашей БД.

    Возвращает:
    - `telegram_id`: Уникальный ID пользователя Telegram.
    - `username`: Имя пользователя Telegram (если есть).
    - `first_name`: Имя пользователя Telegram.
    - `last_name`: Фамилия пользователя Telegram.
    - `is_allowed`: Статус разрешения доступа в вашей системе.
    - `message`: Сообщение о статусе операции.

    Выбрасывает HTTPException:
    - 401 Unauthorized: Если `initData` недействительна (например, подделана или истекла).
    - 400 Bad Request: Если `initData` отсутствует или имеет некорректный формат.
    - **403 Forbidden**: Если пользователь с данным `telegram_id` не найден в вашей базе данных `Users`.
    - 500 Internal Server Error: При внутренних ошибках сервера.
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


@router.get("/get_user")
# НОВА ФУНКЦІЯ ЗАЛЕЖНОСТІ
async def get_current_telegram_user(
    # Припускаємо, що фронтенд відправлятиме initData як кастомний хедер "X-Telegram-Init-Data"
    x_telegram_init_data: str = Header(
        ...,
        alias="X-Telegram-Init-Data",
        description="Дані ініціалізації Telegram Mini App",
    )
    # x_telegram_init_data: str = "user=%7B%22id%22%3A548019148%2C%22first_name%22%3A%22%D0%A1%D0%B5%D1%80%D0%B3%D0%B5%D0%B9%22%2C%22last_name%22%3A%22%D0%9E%D0%BD%D0%B8%D1%89%D0%B5%D0%BD%D0%BA%D0%BE%22%2C%22username%22%3A%22OnyshchenkoSergey%22%2C%22language_code%22%3A%22uk%22%2C%22allows_write_to_pm%22%3Atrue%2C%22photo_url%22%3A%22https%3A%5C%2F%5C%2Ft.me%5C%2Fi%5C%2Fuserpic%5C%2F320%5C%2Fqf0qiya3lYZumE5ExiC55ONcmy-5vzP6pZzzBMV92vw.svg%22%7D&chat_instance=1925380814121275371&chat_type=channel&auth_date=1755268382&signature=-Wek8bfSlr6OOwIVIFYV_5bsXA9Krzzw_I51BXxoIZxn4L0qvcU48b7sgZOPf-AjiQaW1Q5BOkFGG8ekj6ycAw&hash=add338a30c8ad8606d1d303d0a99eb25f95bcf3f4a7a58b34e61f37111215853",
):
    """
    Залежність, яка перевіряє Telegram initData з хедера запиту.
    Якщо дані валідні та користувач знайдений/дозволений у БД, повертає об'єкт користувача з БД.
    Інакше - викидає HTTPException.
    """
    if not x_telegram_init_data:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Відсутній хедер 'X-Telegram-Init-Data'.",
        )

    try:
        parsed_init_data = check_telegram_auth(x_telegram_init_data)
    except HTTPException as e:
        # Перехоплюємо та перевикидаємо помилки, вже визначені в check_telegram_auth
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

    # Перевіряємо, чи користувач існує в нашій базі даних і чи має він доступ
    user_in_db = (
        await Users.objects().where(Users.telegram_id == telegram_id).first().run()
    )

    if not user_in_db or not user_in_db.is_allowed:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Доступ заборонено. Користувач не зареєстрований або не має дозволу.",
        )

    return user_in_db  # Повертаємо об'єкт користувача з БД


@router.post("/auth/login-widget", summary="Авторизація через Telegram Login Widget")
async def login_via_widget(data: TelegramWidgetData):
    """
    Авторизація для браузерних клієнтів через Telegram Login Widget.

    1. Верифікує хеш Widget-даних (SHA256-алгоритм, відмінний від Mini App).
    2. Перевіряє наявність та дозвіл користувача в БД.
    3. Формує та повертає init_data рядок у форматі Mini App,
       сумісний з існуючим заголовком X-Telegram-Init-Data.
    """
    # 1. Верифікуємо хеш
    check_widget_auth(data)

    # 2. Перевіряємо користувача в БД
    user_in_db = (
        await Users.objects().where(Users.telegram_id == data.id).first().run()
    )

    if not user_in_db:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Доступ заборонено. Користувач з Telegram ID {data.id} не зареєстрований.",
        )
    if not user_in_db.is_allowed:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Доступ заборонено. Ваш акаунт не має активного дозволу.",
        )

    # 3. Оновлюємо дані користувача
    current_utc_time = datetime.now(timezone.utc)
    user_in_db.username = data.username
    user_in_db.first_name = data.first_name
    user_in_db.last_name = data.last_name
    user_in_db.last_activity_date = current_utc_time
    await user_in_db.save().run()

    # 4. Формуємо init_data рядок у форматі Mini App
    #    (сумісний з існуючим check_telegram_auth та X-Telegram-Init-Data заголовком)
    import json as _json
    from urllib.parse import urlencode, quote

    user_json = _json.dumps({
        "id": data.id,
        "first_name": data.first_name,
        **({"last_name": data.last_name} if data.last_name else {}),
        **({"username": data.username} if data.username else {}),
        **({"photo_url": data.photo_url} if data.photo_url else {}),
        "language_code": "uk",
    }, ensure_ascii=False, separators=(",", ":"))

    params = {
        "user": user_json,
        "auth_date": str(data.auth_date),
    }

    # Формуємо data_check_string для Mini App підпису
    data_check_string = "\n".join(
        f"{k}={v}" for k, v in sorted(params.items())
    )

    # Рахуємо хеш за Mini App алгоритмом (щоб /get_user міг верифікувати)
    secret_key = hmac.new(
        key=b"WebAppData",
        msg=TELEGRAM_BOT_TOKEN.encode("utf-8"),
        digestmod=hashlib.sha256,
    ).digest()

    new_hash = hmac.new(
        key=secret_key,
        msg=data_check_string.encode("utf-8"),
        digestmod=hashlib.sha256,
    ).hexdigest()

    params["hash"] = new_hash

    # URL-кодуємо так само як Telegram Mini App
    init_data = "&".join(
        f"{k}={quote(str(v), safe='')}" if k == "user" else f"{k}={v}"
        for k, v in params.items()
    )

    print(f"[{current_utc_time}] Widget login: user {data.id} ({data.username}) authorized.")

    return {"init_data": init_data}


# ---------------------------------------------------------------------------
# Bot Deep Link Auth
# ---------------------------------------------------------------------------

# In-memory хранилище токенов (dict: token → info)
# Токены живут 5 минут, затем очищаются при проверке.
login_tokens: dict[str, dict] = {}

TELEGRAM_BOT_NAME = os.getenv("NEXT_PUBLIC_TELEGRAM_BOT_NAME", "EridonKharkiv_bot")


def _build_init_data_for_user(user_in_db) -> str:
    """Генерирует init_data-строку в формате Mini App для пользователя."""
    from urllib.parse import quote as _quote
    user_json = json.dumps({
        "id": user_in_db.telegram_id,
        "first_name": user_in_db.first_name or "",
        **({
            "last_name": user_in_db.last_name
        } if user_in_db.last_name else {}),
        **({
            "username": user_in_db.username
        } if user_in_db.username else {}),
        "language_code": "uk",
    }, ensure_ascii=False, separators=(",", ":"))

    auth_date = str(int(datetime.now(timezone.utc).timestamp()))
    params = {"user": user_json, "auth_date": auth_date}

    data_check_string = "\n".join(
        f"{k}={v}" for k, v in sorted(params.items())
    )
    secret_key = hmac.new(
        key=b"WebAppData",
        msg=TELEGRAM_BOT_TOKEN.encode("utf-8"),
        digestmod=hashlib.sha256,
    ).digest()
    new_hash = hmac.new(
        key=secret_key,
        msg=data_check_string.encode("utf-8"),
        digestmod=hashlib.sha256,
    ).hexdigest()
    params["hash"] = new_hash

    return "&".join(
        f"{k}={_quote(str(v), safe='')}" if k == "user" else f"{k}={v}"
        for k, v in params.items()
    )


async def confirm_login_token(token: str, telegram_id: int) -> bool:
    """
    Вызывается из bot-вебхука после получения /start weblogin_TOKEN.
    Проверяет токен, находит пользователя в БД, генерирует init_data.
    Возвращает True если успешно, False если токен невалиден.
    """
    entry = login_tokens.get(token)
    if not entry or entry["status"] != "pending":
        return False
    if datetime.now(timezone.utc) > entry["expires"]:
        login_tokens.pop(token, None)
        return False

    user_in_db = await Users.objects().where(
        Users.telegram_id == telegram_id
    ).first().run()

    if not user_in_db or not user_in_db.is_allowed:
        login_tokens[token]["status"] = "forbidden"
        return False

    # Генерируем init_data
    init_data = _build_init_data_for_user(user_in_db)
    login_tokens[token].update({
        "status": "confirmed",
        "init_data": init_data,
        "user_id": telegram_id,
    })
    return True


@router.post("/auth/generate-login-token", summary="Генерація токену для Bot Deep Link Login")
async def generate_login_token():
    """
    Створює унікальний токен та повертає deep link для входу через Telegram-бота.
    TTL токена — 5 хвилин.
    """
    # Очищаем просроченные токены
    now = datetime.now(timezone.utc)
    expired = [t for t, v in login_tokens.items() if v["expires"] < now]
    for t in expired:
        login_tokens.pop(t, None)

    token = str(uuid.uuid4()).replace("-", "")[:24]
    login_tokens[token] = {
        "status": "pending",
        "expires": now + timedelta(minutes=5),
        "init_data": None,
        "user_id": None,
    }

    bot_name = os.getenv("TELEGRAM_BOT_NAME", "EridonKharkiv_bot")
    tg_link = f"tg://resolve?domain={bot_name}&start=weblogin_{token}"
    web_link = f"https://t.me/{bot_name}?start=weblogin_{token}"

    return {"token": token, "deep_link": tg_link, "web_link": web_link, "expires_in": 300}


@router.get("/auth/check-login-token/{token}", summary="Перевірка статусу Deep Link-токену")
async def check_login_token(token: str):
    """
    Проверяет статус токена авторизации. Фронтенд поллингует этот эндпоинт каждые 2 секунды.
    - status=pending: пользователь ещё не подтвердил
    - status=confirmed: возвращает init_data
    - status=expired / not_found: токен недействителен
    """
    entry = login_tokens.get(token)
    if not entry:
        return {"status": "not_found"}

    if datetime.now(timezone.utc) > entry["expires"]:
        login_tokens.pop(token, None)
        return {"status": "expired"}

    if entry["status"] == "confirmed":
        init_data = entry["init_data"]
        login_tokens.pop(token, None)  # Одноразовое использование
        return {"status": "confirmed", "init_data": init_data}

    if entry["status"] == "forbidden":
        login_tokens.pop(token, None)
        return {"status": "forbidden"}

    return {"status": "pending"}
