from datetime import datetime, timezone
from aiogram import Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.types import CallbackQuery
from .config import logger, bot, ADMINS_ID
from .telegram_auth import confirm_login_token
from .tables import Users

def setup_bot_handlers(dp: Dispatcher):
    """
    Налаштовує обробники повідомлень для Telegram-бота.
    """
    
    @dp.message(CommandStart())
    async def handle_bot_start(message):
        """Handle /start command"""
        telegram_id = message.from_user.id
        first_name = message.from_user.first_name or ""
        last_name = message.from_user.last_name or ""
        username = message.from_user.username or ""

        # Автоматично зберігаємо / оновлюємо користувача в таблиці Users
        try:
            user = await Users.objects().where(Users.telegram_id == telegram_id).first().run()
            now_utc = datetime.now(timezone.utc)
            if not user:
                user = Users(
                    telegram_id=telegram_id,
                    username=username,
                    first_name=first_name,
                    last_name=last_name,
                    is_allowed=False,
                    is_guest=False,
                    registration_date=now_utc,
                    last_activity_date=now_utc,
                )
                await user.save().run()
                logger.info(f"✅ Додано нового користувача в Users через /start: {telegram_id} (@{username})")

                # Сповіщаємо адміністраторів про нового користувача
                for admin_id in ADMINS_ID:
                    try:
                        await bot.send_message(
                            chat_id=admin_id,
                            text=(
                                f"👤 <b>Новий користувач запустив бота!</b>\n\n"
                                f"Ім'я: <b>{first_name} {last_name}</b>\n"
                                f"Username: @{username}\n"
                                f"Telegram ID: <code>{telegram_id}</code>"
                            ),
                            parse_mode="HTML"
                        )
                    except Exception as err:
                        logger.warning(f"Не вдалося надіслати сповіщення адміну {admin_id}: {err}")
            else:
                user.username = username
                user.first_name = first_name
                user.last_name = last_name
                user.last_activity_date = now_utc
                await user.save().run()
        except Exception as e:
            logger.error(f"❌ Помилка автореєстрації в handle_bot_start: {e}")

        text = message.text or ""
        parts = text.split(" ", 1)
        if len(parts) == 2 and parts[1].startswith("weblogin_"):
            token = parts[1][len("weblogin_"):]
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
                f"Вітаю, {first_name}! 👋\n\n"
                f"Я бот Eridon.\n"
                f"Ваш Telegram ID: <code>{telegram_id}</code> <i>(натисніть, щоб скопіювати)</i>\n\n"
                f"• Якщо ви співробітник або бухгалтер — передайте цей ID адміністратору для налаштування доступу та сповіщень.\n"
                f"• Якщо ви намагаєтесь увійти у веб-додаток з комп'ютера — надішліть сюди 6-значний код з екрана.",
                parse_mode="HTML"
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

    @dp.callback_query(F.data.startswith("np_remind:done:"))
    async def handle_np_remind_done(callback: CallbackQuery):
        """Обробник кнопки 'Готово' у нагадуванні по доставці НП"""
        reminder_id_str = callback.data.split(":", 2)[2]
        try:
            reminder_id = int(reminder_id_str)
            from .services.delivery_reminder_service import mark_reminder_done
            await mark_reminder_done(reminder_id)
        except Exception as e:
            logger.error(f"Помилка оновлення статусу нагадування {reminder_id_str}: {e}")

        try:
            await callback.message.delete()
        except Exception as e:
            logger.warning(f"Не вдалося видалити повідомлення нагадування: {e}")

        try:
            await callback.answer("✅ Нагадування закрито!")
        except Exception:
            pass

    @dp.callback_query(F.data.startswith("np_remind:delay:"))
    async def handle_np_remind_delay(callback: CallbackQuery):
        """Обробник кнопки 'Перенести' у нагадуванні по доставці НП (переносить на 15 хв)"""
        reminder_id_str = callback.data.split(":", 2)[2]
        try:
            reminder_id = int(reminder_id_str)
            from .services.delivery_reminder_service import delay_reminder
            await delay_reminder(reminder_id, delay_minutes=15)
        except Exception as e:
            logger.error(f"Помилка перенесення нагадування {reminder_id_str}: {e}")

        try:
            await callback.message.delete()
        except Exception as e:
            logger.warning(f"Не вдалося видалити повідомлення при перенесенні: {e}")

        try:
            await callback.answer("⏰ Нагадування перенесено на 15 хвилин", show_alert=False)
        except Exception:
            pass

