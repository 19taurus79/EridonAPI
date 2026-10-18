import html
from datetime import datetime, timezone
from aiogram import Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from .config import logger, bot, ADMINS_ID, SUPERADMIN_TELEGRAM_ID
from .telegram_auth import confirm_login_token
from .mcp_linking import confirm_mcp_linking, cancel_mcp_linking
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
        user = None
        now_utc = datetime.now(timezone.utc)
        try:
            user = await Users.objects().where(Users.telegram_id == telegram_id).first().run()
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
            else:
                user.username = username
                user.first_name = first_name
                user.last_name = last_name
                user.last_activity_date = now_utc
                await user.save().run()
        except Exception as e:
            logger.error(f"❌ Помилка автореєстрації в handle_bot_start: {e}")

        # Обробка weblogin_ токенів для авторизації на сайті
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
            return

        # Обробка mcp_ токенів для зв'язування з ChatGPT / MCP Gateway
        if len(parts) == 2 and (parts[1].startswith("mcp_") or parts[1].startswith("link_")):
            nonce = parts[1][4:] if parts[1].startswith("mcp_") else parts[1][5:]
            if len(nonce) > 52:
                await message.answer(
                    "❌ Недійсне посилання для прив'язки: токен перевищує допустиму довжину."
                )
                return

            if not user or not user.is_allowed:
                await message.answer(
                    "❌ Доступ заборонено: ваш акаунт не активовано або заблоковано в системі Eridon."
                )
                return

            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="✅ Підтвердити прив'язку",
                        callback_data=f"mcp_confirm_{nonce}"
                    ),
                    InlineKeyboardButton(
                        text="❌ Скасувати",
                        callback_data=f"mcp_cancel_{nonce}"
                    ),
                ]
            ])
            display_name = html.escape(str(user.full_name_for_orders or first_name or "Користувач"))
            await message.answer(
                "⚠️ <b>Запит на підключення до ChatGPT MCP Gateway</b>\n\n"
                f"👤 Користувач: <b>{display_name}</b>\n"
                f"🆔 Telegram ID: <code>{telegram_id}</code>\n\n"
                "Ви підтверджуєте прив'язку вашого облікового запису Eridon до ChatGPT?",
                reply_markup=keyboard,
                parse_mode="HTML",
            )
            return

        is_pending = not user or not user.is_allowed

        if is_pending:
            # Клавіатура для вибору ролі адміністратором
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text="👑 Адмін", callback_data=f"approve_admin_{telegram_id}"),
                    InlineKeyboardButton(text="👤 Користувач", callback_data=f"approve_user_{telegram_id}"),
                ],
                [
                    InlineKeyboardButton(text="👁 Гість", callback_data=f"approve_guest_{telegram_id}"),
                    InlineKeyboardButton(text="❌ Відмовити", callback_data=f"reject_{telegram_id}"),
                ]
            ])

            target_admin_id = SUPERADMIN_TELEGRAM_ID or (ADMINS_ID[0] if ADMINS_ID else None)
            if target_admin_id:
                try:
                    await bot.send_message(
                        chat_id=target_admin_id,
                        text=(
                            f"👤 <b>Запит на доступ від користувача!</b>\n\n"
                            f"Ім'я: <b>{first_name} {last_name}</b>\n"
                            f"Username: @{username or 'немає'}\n"
                            f"Telegram ID: <code>{telegram_id}</code>\n\n"
                            f"Оберіть роль для надання доступу:"
                        ),
                        reply_markup=keyboard,
                        parse_mode="HTML"
                    )
                except Exception as err:
                    logger.warning(f"Не вдалося надіслати сповіщення суперадміну {target_admin_id}: {err}")

            await message.answer(
                f"Вітаю, {first_name}! 👋\n\n"
                f"Я бот Eridon.\n"
                f"Ваш Telegram ID: <code>{telegram_id}</code> <i>(натисніть, щоб скопіювати)</i>\n\n"
                f"⏳ <b>Ваш запит на доступ надіслано адміністратору.</b>\n"
                f"Очікуйте підтвердження — вам надійде сповіщення в цей чат.",
                parse_mode="HTML"
            )
        else:
            await message.answer(
                f"Вітаю, {first_name}! 👋\n\n"
                f"Я бот Eridon.\n"
                f"Ваш Telegram ID: <code>{telegram_id}</code> <i>(натисніть, щоб скопіювати)</i>\n\n"
                f"✅ У вас є активний доступ до системи.\n"
                f"• Для входу у веб-додаток з комп'ютера — надішліть 6-значний код з екрана.",
                parse_mode="HTML"
            )

    @dp.callback_query(F.data.startswith("approve_admin_") | F.data.startswith("approve_user_") | F.data.startswith("approve_guest_"))
    async def handle_approve_user_callback(callback: CallbackQuery):
        """Обробник надання доступу та вибору ролі адміністратором"""
        # Перевірка: тільки суперадмін може затверджувати доступ
        approver_id = SUPERADMIN_TELEGRAM_ID or (ADMINS_ID[0] if ADMINS_ID else None)
        if approver_id and callback.from_user.id != approver_id:
            await callback.answer("❌ Тільки суперадміністратор може підтверджувати доступ.", show_alert=True)
            return

        parts = callback.data.split("_")
        role = parts[1]  # admin / user / guest
        user_id = int(parts[2])

        is_admin_flag = (role == "admin")
        is_guest_flag = (role == "guest")

        try:
            user = await Users.objects().where(Users.telegram_id == user_id).first().run()
            if user:
                user.is_allowed = True
                user.is_admin = is_admin_flag
                user.is_guest = is_guest_flag
                await user.save().run()
            else:
                user = Users(
                    telegram_id=user_id,
                    is_allowed=True,
                    is_admin=is_admin_flag,
                    is_guest=is_guest_flag,
                    registration_date=datetime.now(timezone.utc),
                    last_activity_date=datetime.now(timezone.utc),
                )
                await user.save().run()

            role_labels = {"admin": "👑 Адмін", "user": "👤 Користувач", "guest": "👁 Гість"}
            role_label = role_labels.get(role, role)

            orig_text = callback.message.html_text or callback.message.text or ""
            await callback.message.edit_text(
                f"{orig_text}\n\n✅ <b>Доступ надано!</b> Роль: {role_label}",
                parse_mode="HTML"
            )

            try:
                await bot.send_message(
                    chat_id=user_id,
                    text=f"🎉 <b>Вам надано доступ!</b> Роль: {role_label}.\nТепер ви можете користуватися системою.",
                    parse_mode="HTML"
                )
            except Exception as e:
                logger.warning(f"Не вдалося сповістити користувача {user_id}: {e}")

            await callback.answer(f"Доступ надано: {role_label}")
        except Exception as e:
            logger.error(f"Помилка при збереженні доступу для {user_id}: {e}")
            await callback.answer("❌ Помилка збереження", show_alert=True)

    @dp.callback_query(F.data.startswith("reject_"))
    async def handle_reject_user_callback(callback: CallbackQuery):
        """Обробник відхилення запиту на доступ"""
        # Перевірка: тільки суперадмін може відхиляти доступ
        approver_id = SUPERADMIN_TELEGRAM_ID or (ADMINS_ID[0] if ADMINS_ID else None)
        if approver_id and callback.from_user.id != approver_id:
            await callback.answer("❌ Тільки суперадміністратор може відхиляти доступ.", show_alert=True)
            return

        user_id = int(callback.data.split("_")[1])
        try:
            user = await Users.objects().where(Users.telegram_id == user_id).first().run()
            if user:
                user.is_allowed = False
                await user.save().run()

            orig_text = callback.message.html_text or callback.message.text or ""
            await callback.message.edit_text(
                f"{orig_text}\n\n❌ <b>У доступі відмовлено.</b>",
                parse_mode="HTML"
            )

            try:
                await bot.send_message(
                    chat_id=user_id,
                    text="❌ Вашу заявку на доступ відхилено адміністратором."
                )
            except Exception as e:
                logger.warning(f"Не вдалося сповістити користувача {user_id}: {e}")

            await callback.answer("Відхилено")
        except Exception as e:
            logger.error(f"Помилка при відхиленні доступу {user_id}: {e}")
            await callback.answer("❌ Помилка", show_alert=True)

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

    @dp.callback_query(F.data.startswith("mcp_confirm_"))
    async def handle_mcp_confirm(callback: CallbackQuery):
        nonce = callback.data[len("mcp_confirm_"):]
        telegram_id = callback.from_user.id

        user = await Users.objects().where(Users.telegram_id == telegram_id).first().run()
        if not user or not user.is_allowed:
            await callback.answer("❌ Доступ заборонено.", show_alert=True)
            return

        success = await confirm_mcp_linking(nonce, telegram_id)
        if success:
            await callback.message.edit_text(
                "✅ <b>Прив'язку успішно підтверджено!</b>\n\n"
                "Тепер ви можете повернутися до ChatGPT та продовжити роботу з MCP Gateway.",
                parse_mode="HTML"
            )
            await callback.answer("Прив'язку успішно підтверджено!")
        else:
            await callback.message.edit_text(
                "❌ <b>Помилка прив'язки:</b> час дії посилання минув або воно вже використане.",
                parse_mode="HTML"
            )
            await callback.answer("Посилання застаріло або недійсне.", show_alert=True)

    @dp.callback_query(F.data.startswith("mcp_cancel_"))
    async def handle_mcp_cancel(callback: CallbackQuery):
        nonce = callback.data[len("mcp_cancel_"):]
        await cancel_mcp_linking(nonce)
        await callback.message.edit_text(
            "❌ <b>Прив'язку до ChatGPT скасовано.</b>",
            parse_mode="HTML"
        )
        await callback.answer("Прив'язку скасовано.")

