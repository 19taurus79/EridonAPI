from pydantic import BaseModel, Field, validator
from typing import List, Dict, Any, Optional
from enum import Enum
import uuid
from datetime import datetime, date


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
    # Дані авто та водія за замовчуванням (для самовивозу "забирає клієнт")
    default_car_make: Optional[str] = None
    default_car_number: Optional[str] = None
    default_trailer_number: Optional[str] = None
    default_driver: Optional[str] = None
    # Новые поля
    default_car_max_weight: Optional[int] = None
    default_car_own_weight: Optional[int] = None
    default_car_length: Optional[float] = None
    default_car_width: Optional[float] = None
    default_car_height: Optional[float] = None
    default_np_data: Optional[Dict[str, Any]] = None

class ChangeDateRequest(BaseModel):
    new_date: date


class Party(BaseModel):
    moved_q: float
    party: str


class DeliveryItem(BaseModel):
    product: str
    quantity: float
    weight: float
    parties: List[Party]


class DeliveryOrder(BaseModel):
    order: str
    items: List[DeliveryItem]


class DeleteDeliveryRequest(BaseModel):
    delivery_id: int


class DeliveryRequest(BaseModel):
    client: str
    manager: str
    address: str
    contact: str = ""   # Не обов'язково при самовивозі
    phone: str = ""     # Не обов'язково при самовивозі
    date: str  # ISO-формат строки
    comment: str
    is_custom_address: bool
    latitude: float
    longitude: float
    total_weight: float
    orders: List[DeliveryOrder]
    status: str = "Створено"  # Статус доставки (напр. "Самовивіз")
    override_created_by: Optional[int] = None  # Перевизначає автора (для розділення доставки адміном)
    actor_name: Optional[str] = None


class UpdateParty(BaseModel):
    party: str
    moved_q: float


class UpdateItem(BaseModel):
    product: str
    nomenclature: str
    quantity: float
    manager: str
    client: str
    order_ref: Optional[str] = Field(None, alias="orderRef")
    weight: float
    parties: List[UpdateParty]


class UpdateDeliveryRequest(BaseModel):
    delivery_id: int
    status: str
    total_weight: Optional[float] = None
    items: List[UpdateItem]
    actor_name: Optional[str] = None


class ChangeDeliveryDateRequest(BaseModel):
    delivery_id: int
    new_date: str


class BatchUpdateDeliveryRequest(BaseModel):
    delivery_ids: List[int]
    status: Optional[str] = None
    new_date: Optional[str] = None


class CommentType(str, Enum):
    """Тип коментаря"""

    ORDER = "order"
    PRODUCT = "product"


class CreateCommentRequest(BaseModel):
    """Запит на створення коментаря"""

    comment_type: CommentType = Field(
        ..., description="Тип коментаря: order або product"
    )
    order_ref: str = Field(..., min_length=1, max_length=50, description="Номер заявки")
    product_id: Optional[str] = Field(None, description="UUID товару (для дашборду)")
    product_name: Optional[str] = Field(
        None, max_length=255, description="Назва товару (для BI)"
    )
    comment_text: str = Field(..., min_length=1, description="Текст коментаря")

    @validator("comment_text")
    def validate_comment_text(cls, v):
        """Валідація тексту коментаря"""
        if not v or not v.strip():
            raise ValueError("Текст коментаря не може бути порожнім")
        return v.strip()

    @validator("product_id", "product_name")
    def validate_product_fields(cls, v, values):
        """Валідація полів товару залежно від типу коментаря"""
        comment_type = values.get("comment_type")

        # Для коментарів заявки product_id та product_name мають бути None
        if comment_type == CommentType.ORDER and v is not None:
            raise ValueError(
                "Для коментарів заявки product_id та product_name мають бути null"
            )

        return v

    @validator("product_name")
    def validate_product_comment(cls, v, values):
        """Для коментарів товару хоча б одне поле має бути заповнене"""
        comment_type = values.get("comment_type")
        product_id = values.get("product_id")

        if comment_type == CommentType.PRODUCT:
            if not product_id and not v:
                raise ValueError(
                    "Для коментарів товару product_id або product_name обов'язкові"
                )

        return v

    class Config:
        json_schema_extra = {
            "example": {
                "comment_type": "product",
                "order_ref": "ТЕ-00071300",
                "product_id": "9aa0c0fc-1239-42cb-a4ec-59c614d77423",
                "product_name": "Аклон 60%, к.с. (5 л)",
                "comment_text": "Потрібно терміново відвантажити",
            }
        }


class UpdateCommentRequest(BaseModel):
    """Запит на оновлення коментаря"""

    comment_text: str = Field(..., min_length=1, description="Новий текст коментаря")

    @validator("comment_text")
    def validate_comment_text(cls, v):
        if not v or not v.strip():
            raise ValueError("Текст коментаря не може бути порожнім")
        return v.strip()

    class Config:
        json_schema_extra = {"example": {"comment_text": "Оновлений текст коментаря"}}


class CommentResponse(BaseModel):
    """Відповідь з даними коментаря"""

    id: int = Field(..., description="ID коментаря")
    comment_type: CommentType = Field(..., description="Тип коментаря")
    order_ref: str = Field(..., description="Номер заявки")
    product_id: Optional[uuid.UUID] = Field(None, description="UUID товару")
    product_name: Optional[str] = Field(None, description="Назва товару")
    comment_text: str = Field(..., description="Текст коментаря")
    created_by: int = Field(..., description="Telegram ID автора")
    created_by_name: str = Field(..., description="Ім'я автора")
    created_at: datetime = Field(..., description="Дата створення")
    updated_at: Optional[datetime] = Field(None, description="Дата оновлення")

    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "id": 123,
                "comment_type": "product",
                "order_ref": "ТЕ-00071300",
                "product_id": "9aa0c0fc-1239-42cb-a4ec-59c614d77423",
                "product_name": "Аклон 60%, к.с. (5 л)",
                "comment_text": "Потрібно терміново відвантажити",
                "created_by": 123456789,
                "created_by_name": "Іван Петренко",
                "created_at": "2026-02-02T12:00:00.000Z",
                "updated_at": None,
            }
        }


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
