# app/telegram_auth.py
import hmac, hashlib, json, uuid
from urllib.parse import parse_qsl
from datetime import datetime, timezone, timedelta

from fastapi import HTTPException, status, APIRouter, Header, Depends, Body
from pydantic import BaseModel
from dotenv import load_dotenv
import os

from .tables import Users  # Импорт вашей модели Users
from .config import (
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_WIDGET_BOT_TOKEN,
    logger
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
    dev_mode = os.getenv("DEV_MODE", "false").lower() == "true"

    if not TELEGRAM_BOT_TOKEN and not dev_mode:
        raise RuntimeError(
            "TELEGRAM_BOT_TOKEN не установлен. Проверьте config.py или переменные окружения."
        )

    parsed = dict(parse_qsl(init_data))
    hash_ = parsed.pop("hash", None)

    if not hash_ and not dev_mode:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Отсутствует 'hash' в данных инициализации Telegram.",
        )

    # В DEV_MODE пропускаем проверку подписи
    if dev_mode:
        if TELEGRAM_BOT_TOKEN and TELEGRAM_BOT_TOKEN != "MOCK":
             logger.warning(f"[SECURITY] DEV_MODE is active for user {parsed.get('id')} while a real bot token is set!")
        else:
             logger.info(f"[DEV_MODE] Skipping signature check for user {parsed.get('id')}")
        return parsed

    sorted_items = sorted(parsed.items())
    data_check_string_parts = []
    for k, v in sorted_items:
        part = f"{k}={v}"
        data_check_string_parts.append(part)

    data_check_string = "\n".join(data_check_string_parts)

    # Попытка №1: проверка с использованием основного TELEGRAM_BOT_TOKEN
    secret_key_main = hmac.new(
        key=b"WebAppData",
        msg=TELEGRAM_BOT_TOKEN.encode("utf-8"),
        digestmod=hashlib.sha256,
    ).digest()

    calculated_hash_main = hmac.new(
        key=secret_key_main,
        msg=data_check_string.encode("utf-8"),
        digestmod=hashlib.sha256,
    ).hexdigest()

    is_valid = (calculated_hash_main == hash_)

    # Попытка №2: проверка с использованием TELEGRAM_WIDGET_BOT_TOKEN (если они отличаются)
    if not is_valid and TELEGRAM_WIDGET_BOT_TOKEN and TELEGRAM_WIDGET_BOT_TOKEN != TELEGRAM_BOT_TOKEN:
        secret_key_widget = hmac.new(
            key=b"WebAppData",
            msg=TELEGRAM_WIDGET_BOT_TOKEN.encode("utf-8"),
            digestmod=hashlib.sha256,
        ).digest()

        calculated_hash_widget = hmac.new(
            key=secret_key_widget,
            msg=data_check_string.encode("utf-8"),
            digestmod=hashlib.sha256,
        ).hexdigest()

        is_valid = (calculated_hash_widget == hash_)
        if is_valid:
            logger.info("Telegram signature successfully verified using TELEGRAM_WIDGET_BOT_TOKEN")

    if not is_valid:
        # Логируем несовпадение хэшей для упрощения отладки на продакшене
        token_main_preview = TELEGRAM_BOT_TOKEN[:8] + "..." if TELEGRAM_BOT_TOKEN else "None"
        token_widget_preview = TELEGRAM_WIDGET_BOT_TOKEN[:8] + "..." if TELEGRAM_WIDGET_BOT_TOKEN else "None"
        
        logger.warning(
            f"[AUTH_ERROR] Hash validation failed!\n"
            f"Expected hash: {hash_}\n"
            f"Calculated with TELEGRAM_BOT_TOKEN ({token_main_preview}): {calculated_hash_main}\n"
            f"Data check string:\n{data_check_string}"
        )
        
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
    logger.info(
        f"[{datetime.now(timezone.utc)}] Получены RAW INIT DATA: {data.initData[:100]}..."
    )

    try:
        parsed_init_data = check_telegram_auth(data.initData)
    except HTTPException as e:
        logger.info(
            f"[{datetime.now(timezone.utc)}] Ошибка аутентификации initData: {e.detail}"
        )
        raise e
    except Exception as e:
        logger.info(
            f"[{datetime.now(timezone.utc)}] Неожиданная ошибка при проверке initData: {e}"
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Внутренняя ошибка сервера при проверке данных: {e}",
        )

    user_info_str = parsed_init_data.get("user")
    if not user_info_str:
        logger.info(f"[{datetime.now(timezone.utc)}] Отсутствует поле 'user' в initData.")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="В данных инициализации Telegram отсутствует информация о пользователе (поле 'user').",
        )

    try:
        user_data = json.loads(user_info_str)
    except json.JSONDecodeError:
        logger.info(
            f"[{datetime.now(timezone.utc)}] Неверный формат JSON для данных пользователя: {user_info_str[:50]}..."
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Неверный формат JSON для данных пользователя в initData.",
        )

    telegram_id = user_data.get("id")
    if not isinstance(telegram_id, int):
        logger.info(
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
        logger.info(
            f"[{current_utc_time}] Пользователь с Telegram ID {telegram_id} НЕ НАЙДЕН в БД. Создаем новую запись с is_allowed=False."
        )
        
        # Автоматическая регистрация нового пользователя без доступа
        new_user = Users(
            telegram_id=telegram_id,
            username=user_data.get("username"),
            first_name=user_data.get("first_name"),
            last_name=user_data.get("last_name"),
            is_allowed=False,
            is_guest=False
        )
        await new_user.save().run()

        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Доступ запрещен. Ваш аккаунт записан в систему, но ожидает подтверждения администратором.",
        )
    if not user_in_db.is_allowed:
        logger.info(
            f"[{current_utc_time}] Пользователь {user_in_db.username or user_in_db.telegram_id} (ID: {user_in_db.telegram_id}) найден, но is_allowed=False. Доступ запрещен."
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Доступ запрещен. Ваш аккаунт (Telegram ID {telegram_id}) не имеет активного разрешения. Обратитесь к администратору.",
        )

    logger.info(
        f"[{current_utc_time}] Пользователь с Telegram ID {telegram_id} найден. Обновляем данные."
    )
    user_in_db.username = user_data.get("username")
    user_in_db.first_name = user_data.get("first_name")
    user_in_db.last_name = user_data.get("last_name")
    user_in_db.last_activity_date = current_utc_time
    await user_in_db.save().run()
    message = "Данные пользователя успешно обновлены."
    logger.info(
        f"[{current_utc_time}] Данные пользователя {user_in_db.username or user_in_db.telegram_id} (ID: {user_in_db.telegram_id}) обновлены."
    )

    return {
        "message": message,
        "telegram_id": user_in_db.telegram_id,
        "username": user_in_db.username,
        "first_name": user_in_db.first_name,
        "last_name": user_in_db.last_name,
        "is_allowed": user_in_db.is_allowed,
        "is_guest": getattr(user_in_db, "is_guest", False),
        "is_admin": user_in_db.is_admin,
        "full_name_for_orders": user_in_db.full_name_for_orders,
    }


@router.get("/get_user")
# ФУНКЦІЯ ЗАЛЕЖНОСТІ З ПІДТРИМКОЮ TELEGRAM ТА MCP GATEWAY DELEGATION
async def get_current_telegram_user(
    x_telegram_init_data: str | None = Header(
        None,
        alias="X-Telegram-Init-Data",
        description="Дані ініціалізації Telegram Mini App",
    ),
    authorization: str | None = Header(
        None,
        alias="Authorization",
        description="Bearer токен для MCP Gateway / сервісної авторизації",
    ),
):
    """
    Залежність, яка перевіряє аутентифікацію запиту:
    1. Захист від колізій: одночасна передача двох різних методів авторизації заборонена (400 Bad Request).
    2. MCP Gateway / Bearer JWT delegation: перевіряє EdDSA токен, витягує telegram_id (sub).
    3. Telegram Mini App initData: перевіряє двохетапний HMAC-SHA256 підпис.
    4. Перевіряє наявність користувача та is_allowed в БД (403 Forbidden).
    5. Повертає об'єкт користувача з БД.
    """
    # 1. Collision check: reject ambiguous credentials
    if x_telegram_init_data and authorization:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Одночасне використання декількох методів авторизації заборонено.",
        )

    # 2. Neither header provided
    if not x_telegram_init_data and not authorization:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Відсутній заголовок авторизації ('X-Telegram-Init-Data' або 'Authorization').",
        )

    # 3. Path A: Authorization Bearer (MCP Gateway delegation)
    if authorization:
        parts = authorization.strip().split()
        if len(parts) != 2 or parts[0].lower() != "bearer":
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Невірний формат заголовка Authorization. Очікується 'Bearer <token>'.",
            )
        bearer_token = parts[1]
        try:
            from .jwt_verification import (
                verify_delegation_token,
                get_verification_key_store,
                JWTVerificationError,
            )
            claims = verify_delegation_token(bearer_token, get_verification_key_store())
            sub = claims.get("sub")
            telegram_id = int(sub)
        except JWTVerificationError as e:
            logger.warning(f"[AUTH_ERROR] MCP delegation token verification failed: {e}")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=f"Помилка авторизації токена: {e}",
            )
        except (ValueError, TypeError):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Невірний формат Telegram ID (sub) у токені.",
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

    # 4. Path B: Existing Telegram Mini App Init Data
    try:
        parsed_init_data = check_telegram_auth(x_telegram_init_data)
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.info(
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


async def check_not_guest(
    current_user=Depends(get_current_telegram_user),
):
    """
    Залежність для перевірки, що користувач не є гостем (is_guest=False).
    Використовується для захисту ендпоінтів, що змінюють дані (POST, PUT, DELETE).
    """
    if getattr(current_user, "is_guest", False):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Доступ заборонено (тільки для читання). Гостьовий акаунт не має прав на зміну даних.",
        )
    return current_user


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

    logger.info(f"[{current_utc_time}] Widget login: user {data.id} ({data.username}) authorized.")

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

    import secrets
    import string
    
    # Generate a unique 6-digit numeric code
    while True:
        token = "".join(secrets.choice(string.digits) for _ in range(6))
        if token not in login_tokens:
            break
            
    login_tokens[token] = {
        "status": "pending",
        "expires": now + timedelta(minutes=5),
        "init_data": None,
        "user_id": None,
    }

    bot_name = os.getenv("NEXT_PUBLIC_TELEGRAM_BOT_NAME", "EridonKharkiv_bot")
    tg_link = f"tg://resolve?domain={bot_name}"
    web_link = f"https://t.me/{bot_name}"

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

class ConfirmTokenRequest(BaseModel):
    token: str
    telegram_id: int

@router.post("/auth/confirm-login-token", summary="Підтвердження токену від телеграм-бота")
async def api_confirm_login_token(req: ConfirmTokenRequest):
    """
    Ендпоінт для окремого бота, щоб підтвердити 6-значний код або weblogin_token.
    Бот відправляє сюди токен та telegram_id. 
    """
    success = await confirm_login_token(req.token, req.telegram_id)
    return {"success": success}
