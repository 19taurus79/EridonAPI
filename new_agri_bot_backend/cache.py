# app/cache.py
import time
from collections import OrderedDict
from functools import wraps
from typing import Any, Optional
from .config import logger, USE_CACHE

class InMemoryCache:
    def __init__(self, max_size: int = 500, default_ttl: int = 3600):
        """
        Инициализация in-memory кэша.
        max_size: Максимальное количество элементов в кэше (LRU-лимит).
        default_ttl: Время жизни кэша по умолчанию в секундах (1 час).
        """
        self.max_size = max_size
        self.default_ttl = default_ttl
        # Хранит пары key -> (value, expire_time)
        self._cache: OrderedDict = OrderedDict()

    def get(self, key: str) -> Optional[Any]:
        if key not in self._cache:
            return None
        
        value, expire_time = self._cache[key]
        if expire_time is not None and time.time() > expire_time:
            # Срок действия истек
            self._cache.pop(key)
            return None
        
        # Переносим в конец очереди (как недавно использованный)
        self._cache.move_to_end(key)
        return value

    def set(self, key: str, value: Any, ttl: Optional[int] = None):
        if len(self._cache) >= self.max_size:
            # Удаляем самый старый элемент (первый)
            self._cache.popitem(last=False)
            
        ttl = ttl if ttl is not None else self.default_ttl
        expire_time = time.time() + ttl if ttl > 0 else None
        self._cache[key] = (value, expire_time)
        self._cache.move_to_end(key)

    def clear(self):
        self._cache.clear()
        logger.info("🧹 In-memory кэш бэкенда полностью очищен.")

# Глобальный инстанс кэша
db_cache = InMemoryCache()

def serialize_arg(val: Any) -> str:
    """Рекурсивно сериализует аргументы функции для генерации уникального ключа кэша."""
    if val is None:
        return "None"
    # Для ORM-моделей пользователей Piccolo/SQLAlchemy
    if hasattr(val, "telegram_id"):
        return f"User({getattr(val, 'telegram_id')})"
    # Если это словарь (например, словарь данных пользователя)
    if isinstance(val, dict):
        if "telegram_id" in val:
            return f"User({val['telegram_id']})"
        return str(sorted((k, serialize_arg(v)) for k, v in val.items()))
    # Если это список, кортеж или множество
    if isinstance(val, (list, tuple, set)):
        return str([serialize_arg(x) for x in val])
    return str(val)

def generate_key(func_name: str, args: tuple, kwargs: dict) -> str:
    """Генерирует стабильный ключ кэша на основе имени функции и аргументов."""
    serialized_args = [serialize_arg(arg) for arg in args]
    serialized_kwargs = {}
    for k, v in kwargs.items():
        # Исключаем служебные аргументы FastAPI, которые не влияют на возвращаемые данные
        if k in ("request", "background_tasks", "websocket"):
            continue
        serialized_kwargs[k] = serialize_arg(v)
    
    # Сортируем kwargs, чтобы порядок аргументов не влиял на ключ
    kwargs_str = str(sorted(serialized_kwargs.items()))
    return f"endpoint:{func_name}:{serialized_args}:{kwargs_str}"

def cached_endpoint(ttl: Optional[int] = None):
    """
    Декоратор для кэширования ответов эндпоинтов FastAPI.
    ttl: время жизни кэша для данного эндпоинта в секундах (None - по умолчанию).
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            if not USE_CACHE:
                start_time = time.perf_counter()
                result = await func(*args, **kwargs)
                elapsed_ms = (time.perf_counter() - start_time) * 1000
                logger.info(f"ℹ️ Кэш отключен. Запрос к БД для {func.__name__} выполнен за {elapsed_ms:.2f} мс.")
                return result
            
            key = generate_key(func.__name__, args, kwargs)
            
            start_time = time.perf_counter()
            cached_value = db_cache.get(key)
            if cached_value is not None:
                elapsed_ms = (time.perf_counter() - start_time) * 1000
                logger.info(f"⚡ Кэш сработал для эндпоинта {func.__name__} (взят из RAM за {elapsed_ms:.2f} мс). Ключ: {key}")
                return cached_value
            
            logger.info(f"🐢 Кэш промахнулся для эндпоинта {func.__name__}. Делаем запрос к БД...")
            start_time_db = time.perf_counter()
            result = await func(*args, **kwargs)
            elapsed_ms_db = (time.perf_counter() - start_time_db) * 1000
            
            db_cache.set(key, result, ttl=ttl)
            logger.info(f"💾 Запрос к БД для {func.__name__} сохранен в кэш. Время выполнения: {elapsed_ms_db:.2f} мс.")
            return result
        return wrapper
    return decorator
