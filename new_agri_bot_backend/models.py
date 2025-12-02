from pydantic import BaseModel
from typing import List, Dict, Any, Optional


# Pydantic-модели используются для валидации данных,
# которые приложение получает и отправляет.
# Это обеспечивает надежность и предсказуемость API.
class SelectedMovedItem(BaseModel):
    index: int
    quantity: float


class ManualMatchInput(BaseModel):
    """
    Модель для данных, которые фронтенд отправляет при ручном сопоставлении.
    Она описывает, какую заявку (request_id) и какие строки из
    таблиц "Перемещено" и "Примечания" пользователь выбрал для сопоставления.
    """

    request_id: str
    # selected_moved_indices: List[int]
    selected_moved_items: List[SelectedMovedItem]
    selected_notes_indices: List[int]


class UploadResponse(BaseModel):
    """
    Модель ответа после успешной загрузки и первичной обработки файлов.
    Возвращает уникальный ID сессии и словарь с данными,
    которые не удалось сопоставить автоматически (`leftovers`).
    """

    session_id: str
    leftovers: Dict[str, Any]


class MatchResponse(BaseModel):
    """
    Модель ответа для эндпоинта ручного сопоставления.
    Сообщает об успехе операции и возвращает ID текущей сессии.
    """

    message: str
    session_id: str


# НОВАЯ МОДЕЛЬ для описания несопоставленных остатков по одной заявке
class UnmatchedData(BaseModel):
    unmatched_moved: List[Dict[str, Any]]
    unmatched_notes: List[Dict[str, Any]]


class ResultsResponse(BaseModel):
    """
    ИЗМЕНЕННАЯ МОДЕЛЬ для финального ответа.
    Теперь содержит `unmatched_by_request` для группировки остатков по заявкам.
    """

    matched_data: List[Dict[str, Any]]
    unmatched_by_request: Dict[str, UnmatchedData]  # Ключ - это request_id


class RegionResponse(BaseModel):
    level_1_id: str
    name: str


class AddressResponse(BaseModel):

    name: str
    category: Optional[str] = None
    full_address: Optional[str] = None

    region: Optional[str] = None
    district: Optional[str] = None
    community: Optional[str] = None


class AddressCreate(BaseModel):

    client: str
    manager: str
    representative: str
    phone1: str
    phone2: Optional[str] = None
    address: str
    latitude: float
    longitude: float
