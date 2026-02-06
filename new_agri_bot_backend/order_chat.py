from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional
from uuid import UUID

from new_agri_bot_backend.tables import OrderChatMessage, Users
from new_agri_bot_backend.telegram_auth import get_current_telegram_user


class ChatMessageBase(BaseModel):
    message_text: str = Field(..., min_length=1, max_length=5000)
    reply_to_message_id: Optional[UUID] = None


class CreateChatMessageRequest(ChatMessageBase):
    order_ref: str = Field(..., min_length=1, max_length=50)


class UpdateChatMessageRequest(BaseModel):
    message_text: str = Field(..., min_length=1, max_length=5000)


class ChatMessageResponse(BaseModel):
    id: UUID
    order_ref: str
    user_id: int
    user_name: str
    message_text: str
    created_at: datetime
    updated_at: datetime
    is_edited: bool
    reply_to_message_id: Optional[UUID] = None

    class Config:
        from_attributes = True


from fastapi import APIRouter, HTTPException, Depends
from typing import List
from uuid import UUID
from datetime import datetime

router = APIRouter(
    prefix="/orders", tags=["chat"], dependencies=[Depends(get_current_telegram_user)]
)


# Отримання повідомлень
@router.get("/{order_ref}/chat/messages", response_model=List[ChatMessageResponse])
async def get_chat_messages(
    order_ref: str, user_data: dict = Depends(get_current_telegram_user)
):
    """Отримати всі повідомлення чату для заявки"""
    messages = (
        await OrderChatMessage.select()
        .where(OrderChatMessage.order_ref == order_ref)
        .order_by(OrderChatMessage.created_at, ascending=True)
    )

    return messages


# Створення повідомлення
@router.post("/{order_ref}/chat/messages", response_model=ChatMessageResponse)
async def create_chat_message(
    order_ref: str,
    request: CreateChatMessageRequest,
    user_data: dict = Depends(get_current_telegram_user),
):
    """Створити нове повідомлення в чаті"""
    # Отримати дані користувача з БД
    user = (
        await Users.select()
        .where(Users.telegram_id == user_data["telegram_id"])
        .first()
    )

    if not user:
        raise HTTPException(status_code=404, detail="Користувача не знайдено")

    message = OrderChatMessage(
        order_ref=order_ref,
        user_id=user["telegram_id"],
        user_name=user["full_name_for_orders"],
        message_text=request.message_text,
        reply_to_message_id=request.reply_to_message_id,
    )

    await message.save()

    # Refresh для отримання auto-generated полів
    saved_message = (
        await OrderChatMessage.select().where(OrderChatMessage.id == message.id).first()
    )

    return saved_message


# Редагування повідомлення
@router.put(
    "/{order_ref}/chat/messages/{message_id}", response_model=ChatMessageResponse
)
async def update_chat_message(
    order_ref: str,
    message_id: UUID,
    request: UpdateChatMessageRequest,
    user_data: dict = Depends(get_current_telegram_user),
):
    """Редагувати повідомлення (тільки автор)"""
    message = (
        await OrderChatMessage.select()
        .where(
            (OrderChatMessage.id == message_id)
            & (OrderChatMessage.order_ref == order_ref)
        )
        .first()
    )

    if not message:
        raise HTTPException(status_code=404, detail="Повідомлення не знайдено")

    # Перевірка, чи користувач є автором
    if message["user_id"] != user_data["telegram_id"]:
        raise HTTPException(
            status_code=403, detail="Ви можете редагувати тільки свої повідомлення"
        )

    await OrderChatMessage.update(
        {
            OrderChatMessage.message_text: request.message_text,
            OrderChatMessage.is_edited: True,
            OrderChatMessage.updated_at: datetime.utcnow(),
        }
    ).where(OrderChatMessage.id == message_id)

    # Отримати оновлене повідомлення
    updated_message = (
        await OrderChatMessage.select().where(OrderChatMessage.id == message_id).first()
    )

    return updated_message


# Видалення повідомлення
@router.delete("/{order_ref}/chat/messages/{message_id}")
async def delete_chat_message(
    order_ref: str,
    message_id: UUID,
    user_data: dict = Depends(get_current_telegram_user),
):
    """Видалити повідомлення (тільки автор або адмін)"""
    message = (
        await OrderChatMessage.select()
        .where(
            (OrderChatMessage.id == message_id)
            & (OrderChatMessage.order_ref == order_ref)
        )
        .first()
    )

    if not message:
        raise HTTPException(status_code=404, detail="Повідомлення не знайдено")

    # Отримати дані користувача
    user = (
        await Users.select()
        .where(Users.telegram_id == user_data["telegram_id"])
        .first()
    )

    # Перевірка прав (автор або адмін)
    if message["user_id"] != user_data["telegram_id"] and not user["is_admin"]:
        raise HTTPException(status_code=403, detail="Недостатньо прав")

    await OrderChatMessage.delete().where(OrderChatMessage.id == message_id)

    return {"message": "Повідомлення видалено"}
