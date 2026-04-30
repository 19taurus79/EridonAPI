from aiogram import Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.types import CallbackQuery
from .config import logger
from .telegram_auth import confirm_login_token

def setup_bot_handlers(dp: Dispatcher):
    """
    Налаштовує обробники повідомлень для Telegram-бота.
    """
    
    @dp.message(CommandStart())
    async def handle_bot_start(message):
        """Handle /start command"""
        text = message.text or ""
        parts = text.split(" ", 1)
        if len(parts) == 2 and parts[1].startswith("weblogin_"):
            token = parts[1][len("weblogin_"):]
            telegram_id = message.from_user.id
            success = await confirm_login_token(token, telegram_id)
            if success:
                await message.answer(
                    "✅ Вхід підтверджено! Поверніться в браузер — сторінка завантажиться автоматично."
                )
            else:
                await message.answer(
                    "❌ Посилання не знайдено або вже використано. Спробуйте ще раз."
                )
        else:
            await message.answer(
                "Вітаю! Я бот авторизації Eridon.\n\n"
                "Якщо ви намагаєтесь увійти в систему, відправте мені 6-значний код з екрану."
            )

    @dp.message(F.text.regexp(r"^\d{6}$"))
    async def handle_login_code(message):
        """Handle 6-digit login code."""
        token = message.text
        telegram_id = message.from_user.id
        
        success = await confirm_login_token(token, telegram_id)
        if success:
            await message.answer(
                "✅ Вхід підтверджено! Поверніться в браузер — сторінка завантажиться автоматично."
            )
        else:
            await message.answer(
                "❌ Код не знайдено або він вже застарів. Спробуйте згенерувати новий код."
            )

    @dp.callback_query(F.data == "delete_msg")
    async def handle_delete_msg_callback(callback: CallbackQuery):
        """Видаляє повідомлення при натисканні на кнопку 'Видалити'"""
        try:
            await callback.message.delete()
        except Exception as e:
            logger.error(f"Помилка при видаленні повідомлення через кнопку: {e}")
        
        try:
            await callback.answer()
        except Exception:
            pass
