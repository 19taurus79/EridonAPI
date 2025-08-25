# app/telegram_auth.py
import hmac, hashlib, json
from urllib.parse import parse_qsl
from datetime import datetime, timezone

from fastapi import HTTPException, status, APIRouter, Header, Depends
from pydantic import BaseModel
from dotenv import load_dotenv
import os

from .tables import Users  # Импорт вашей модели Users
from .config import (
    TELEGRAM_BOT_TOKEN,
)  # Убедитесь, что TELEGRAM_BOT_TOKEN есть в config.py

load_dotenv()

# Если TELEGRAM_BOT_TOKEN также загружается здесь, убедитесь, что он один источник истины,
# либо просто импортируйте из config.py
# TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN") # Лучше брать из config.py

router = APIRouter(tags=["Аутентифікація"])


class InitDataModel(BaseModel):
    initData: str


def check_telegram_auth(init_data: str) -> dict:
    """
    Проверяет init_data, полученные от Telegram Mini App,
    строго следуя предоставленному алгоритму двухэтапного HMAC-SHA256.
    """
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError(
            "TELEGRAM_BOT_TOKEN не установлен. Проверьте config.py или переменные окружения."
        )

    print("\n--- НАЧАЛО ДЕТАЛЬНОЙ ОТЛАДКИ check_telegram_auth ---")
    print(f"1. Получена init_data: '{init_data}'")
    print(f"2. Длина init_data: {len(init_data)}")
    print(
        f"3. TELEGRAM_BOT_TOKEN (обрезан): '{TELEGRAM_BOT_TOKEN[:5]}...{TELEGRAM_BOT_TOKEN[-5:]}'"
    )
    print(f"4. Длина TELEGRAM_BOT_TOKEN: {len(TELEGRAM_BOT_TOKEN)}")

    parsed = dict(parse_qsl(init_data))
    hash_ = parsed.pop("hash", None)
    if not hash_:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Отсутствует 'hash' в данных инициализации Telegram.",
        )

    print(f"5. Извлечен 'hash_': '{hash_}'")
    print(f"6. Распарсенные данные (после удаления 'hash'): {parsed}")
    print(f"7. Количество полей для проверки: {len(parsed)}")

    sorted_items = sorted(parsed.items())
    data_check_string_parts = []
    for k, v in sorted_items:
        part = f"{k}={v}"
        data_check_string_parts.append(part)

    data_check_string = "\n".join(data_check_string_parts)

    print(f"8. Сформированная data_check_string (длина {len(data_check_string)}):")
    print("--- НАЧАЛО data_check_string ---")
    print(data_check_string)
    print("--- КОНЕЦ data_check_string ---")

    secret_key_intermediate = hmac.new(
        key=b"WebAppData",
        msg=TELEGRAM_BOT_TOKEN.encode("utf-8"),
        digestmod=hashlib.sha256,
    ).digest()

    print(
        f"9. Промежуточный секретный ключ (HMAC(WebAppData, bot_token)) (hex): {secret_key_intermediate.hex()}"
    )

    calculated_hash = hmac.new(
        key=secret_key_intermediate,
        msg=data_check_string.encode("utf-8"),
        digestmod=hashlib.sha256,
    ).hexdigest()

    print(f"10. Вычисленный финальный хэш: '{calculated_hash}'")
    print(f"11. Хэши совпадают? (Calculated == Received) : {calculated_hash == hash_}")
    print("--- КОНЕЦ ДЕТАЛЬНОЙ ОТЛАДКИ check_telegram_auth ---\n")

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
    # x_telegram_init_data: str = Header(
    #     ...,
    #     alias="X-Telegram-Init-Data",
    #     description="Дані ініціалізації Telegram Mini App",
    # )
    x_telegram_init_data: str = "user=%7B%22id%22%3A548019148%2C%22first_name%22%3A%22%D0%A1%D0%B5%D1%80%D0%B3%D0%B5%D0%B9%22%2C%22last_name%22%3A%22%D0%9E%D0%BD%D0%B8%D1%89%D0%B5%D0%BD%D0%BA%D0%BE%22%2C%22username%22%3A%22OnyshchenkoSergey%22%2C%22language_code%22%3A%22uk%22%2C%22allows_write_to_pm%22%3Atrue%2C%22photo_url%22%3A%22https%3A%5C%2F%5C%2Ft.me%5C%2Fi%5C%2Fuserpic%5C%2F320%5C%2Fqf0qiya3lYZumE5ExiC55ONcmy-5vzP6pZzzBMV92vw.svg%22%7D&chat_instance=1925380814121275371&chat_type=channel&auth_date=1755268382&signature=-Wek8bfSlr6OOwIVIFYV_5bsXA9Krzzw_I51BXxoIZxn4L0qvcU48b7sgZOPf-AjiQaW1Q5BOkFGG8ekj6ycAw&hash=add338a30c8ad8606d1d303d0a99eb25f95bcf3f4a7a58b34e61f37111215853",
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
