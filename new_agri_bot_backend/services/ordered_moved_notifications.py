import asyncio
import os
import json

import pandas as pd
from aiogram import Bot
from pandas import Timestamp

from new_agri_bot_backend.config import TELEGRAM_BOT_TOKEN, ADMINS_ID, logger
from new_agri_bot_backend.services.send_telegram_notification import send_notification
from new_agri_bot_backend.tables import Submissions, Users


async def split_message_into_chunks(
    text: str, chunk_size: int = 4000
) -> list[str]:
    """
    Разбивает длинный текст на несколько частей, не разрывая строки.
    """
    if len(text) <= chunk_size:
        return [text]

    chunks = []
    current_chunk = ""
    for line in text.split("\n"):
        # Если строка сама по себе длиннее лимита, ее нужно принудительно разбить
        if len(line) > chunk_size:
            # Добавляем текущий накопленный чанк, если он есть
            if current_chunk:
                chunks.append(current_chunk)
                current_chunk = ""
            # Разбиваем слишком длинную строку
            for i in range(0, len(line), chunk_size):
                chunks.append(line[i : i + chunk_size])
            continue

        # Проверяем, поместится ли следующая строка в текущий чанк
        if len(current_chunk) + len(line) + 1 > chunk_size:
            chunks.append(current_chunk)
            current_chunk = line
        else:
            if current_chunk:  # Добавляем перенос строки, если чанк не пустой
                current_chunk += "\n"
            current_chunk += line

    if current_chunk:
        chunks.append(current_chunk)

    return chunks


async def get_data_from_df(frame: pd.DataFrame):
    """
    Принимает DataFrame, извлекает из него уникальные номера контрактов
    и запрашивает по ним данные о менеджерах и клиентах из БД.
    Использует связку (контракт + товар) для точной идентификации клиента.
    """
    # Извлекаем уникальные контракты и товары из DataFrame
    orders = frame["contract"].unique().tolist()
    product_ids = frame["product_id"].unique().tolist()
    
    try:
        data = (
            await Submissions.select(
                Submissions.contract_supplement, 
                Submissions.manager, 
                Submissions.client,
                Submissions.product
            )
            .where(
                (Submissions.contract_supplement.is_in(orders))
                & (Submissions.product.is_in(product_ids))
                & (Submissions.different > 0)
            )
            .run()
        )
        
        # Создаем мапу с композитным ключом (номер_заказа, id_продукта)
        contract_data_map = {
            (item["contract_supplement"], str(item["product"])): {
                "manager": item["manager"],
                "client": item["client"],
            }
            for item in data
        }
        return contract_data_map
    except Exception as e:
        logger.info(f"!!! Ошибка при получении данных о менеджерах и клиентах из БД: {e}")
        return {}


async def notifications(bot: Bot, frame: pd.DataFrame):
    # Проверяем окружение. Если не 'prod', выводим в консоль вместо отправки.
    app_env = os.getenv("APP_ENV", "dev")

    # 1. Получаем мапу { (контракт, продукт): {manager, client} }
    contract_data_map = await get_data_from_df(frame)
    logger.debug("--- Словарь сопоставления (Контракт, Продукт) -> Менеджер/Клиент ---")
    logger.debug(contract_data_map)

    # 2. Добавляем колонки 'manager' и 'client' в DataFrame, используя композитный ключ
    # Используем .apply для доступа к нескольким колонкам в каждой строке
    def get_info(row, field):
        key = (str(row["contract"]), str(row["product_id"]))
        return contract_data_map.get(key, {}).get(field)

    frame["manager"] = frame.apply(lambda row: get_info(row, "manager"), axis=1)
    frame["client"] = frame.apply(lambda row: get_info(row, "client"), axis=1)

    # Заполняем пропуски, если для какого-то контракта не нашелся менеджер
    frame["manager"] = frame["manager"].fillna("Менеджер не определен")
    frame["client"] = frame["client"].fillna("Клиент не определен")

    # 3. Группируем DataFrame по новой колонке 'manager'
    grouped_by_manager = frame.groupby("manager")
    admin_report_parts = []
    admin_report_parts.append(
        "👑 *Зведений звіт по всім переміщенням*\n" + "=" * 25 + "\n"
    )
    logger.info("--- Обработка данных, сгруппированных по менеджеру ---")
    # 4. Итерируемся по группам
    for manager_name, manager_group_df in grouped_by_manager:
        # Берем только второе слово из ФИО, если оно есть, иначе используем полное имя
        informal_manager_name = (
            manager_name.split(" ")[1] if manager_name else "Коллега"
        )
        # --- Формирование красивого сообщения для Telegram ---
        message_text = f"👋 Доброго дня, *{informal_manager_name}*!\n\n"
        message_text += "🆕 У Вас є нові дані по переміщенню товарів:\n"

        # Группируем записи по номеру заказа для более компактного вида
        for order_id, order_group in manager_group_df.groupby("order"):
            message_text += f"\n\n📄 *Заявка на відвантаження:* `{order_id}`\n"

            # Внутри заказа группируем по клиенту
            for client_name, client_group in order_group.groupby("client"):
                message_text += f"  👤 *Контрагент:* {client_name}\n"

                # Внутри клиента группируем по дополнению (контракту)
                for contract_id, contract_group in client_group.groupby("contract"):
                    message_text += f"    📝 *Доповнення:* `{contract_id}`\n"

                    # Внутри контракта группируем по товару
                    for product_name, product_group in contract_group.groupby(
                        "product"
                    ):
                        message_text += f"      📦 *Товар:* _{product_name}_\n"

                        # Итерируемся по каждой строке (партии/позиции) в рамках одного товара
                        for _, row in product_group.iterrows():
                            date_val = row.get("date")
                            formatted_date = (
                                date_val.strftime("%d.%m.%Y")
                                if pd.notna(date_val)
                                else "не вказано"
                            )

                            message_text += (
                                f"        🏷️ *Партія:* `{row.get('party_sign', 'N/A')}`\n"
                            )
                            message_text += f"        🚚 *Переміщено:* *{row.get('qt_moved', 0):.2f}*\n"
                            # message_text += f"      🛒 *Замовлено:* {row.get('qt_order', 0)}\n"
                            # message_text += f"      📈 *Напрям:* {row.get('line_of_business', 'N/A')}\n"
                            # message_text += f"      🗓️ *Період:* {row.get('period', 'N/A')}\n"
                            # message_text += f"      📅 *Дата:* {formatted_date}\n"
                            message_text += (
                                " " * 8 + "-" * 32 + "\n"
                            )  # Разделитель для партий

        # --- ИСПРАВЛЕНИЕ: Добавляем секцию менеджера в отчет ОДИН РАЗ после формирования ---
        admin_report_parts.append(
            f"\n\n👤 *Менеджер:* `{manager_name}`\n" + "-" * 20 + "\n"
        )
        # Убираем личное приветствие из админской версии
        admin_report_parts.append(message_text.split("\n\n", 1)[-1])

        # Выводим сформированное сообщение (в дальнейшем здесь будет вызов send_notification)
        telegram_id_result = (
            await Users.select(Users.telegram_id)
            .where(Users.full_name_for_orders == manager_name)
            .run()
        )
        # --- ИСПРАВЛЕНИЕ: Правильно извлекаем ID из результата ---
        # .run() возвращает список словарей, нам нужно первое значение.
        telegram_id = (
            telegram_id_result[0]["telegram_id"] if telegram_id_result else None
        )

        try:
            if telegram_id:
                message_chunks = await split_message_into_chunks(message_text)
                for chunk in message_chunks:
                    if app_env == "production":
                        await send_notification(
                            bot=bot,
                            chat_ids=[telegram_id],  # Передаем ID в списке
                            text=chunk,
                        )
                    else:
                        # В режиме разработки просто выводим в консоль
                        logger.info(
                            f"\n--- [DEV] Сообщение для {manager_name} (ID: {telegram_id}) ---"
                        )
                        logger.info(chunk)
                        logger.info(f"--- [DEV] Конец сообщения для {manager_name} ---\n")
            else:
                logger.info(
                    f"!!! Увага: Telegram ID для менеджера '{manager_name}' не знайдено. Сповіщення не відправлено."
                )

        except Exception as e:
            logger.info(f"!!! Ошибка при отправке уведомления менеджеру {manager_name}: {e}")

    # --- ИСПРАВЛЕНИЕ: Отправляем сводный отчет ОДИН РАЗ после завершения цикла по менеджерам ---
    if len(admin_report_parts) > 1:  # Отправляем, только если были данные
        admin_chat_ids = []
        if ADMINS_ID and isinstance(ADMINS_ID, str):
            try:
                parsed_ids = json.loads(ADMINS_ID)
                admin_chat_ids = [int(admin_id) for admin_id in parsed_ids]
            except (json.JSONDecodeError, TypeError):
                logger.info(
                    f'!!! Помилка: Не вдалося розпарсити ADMINS_ID. Перевірте формат у .env файлі. Очікується формат ["id1", "id2"].'
                )

        try:
            if not admin_chat_ids:
                logger.info(
                    "!!! Увага: Не знайдено жодного адміністратора для відправки звіту."
                )
                return

            admin_full_report = "".join(admin_report_parts).strip()
            report_chunks = await split_message_into_chunks(admin_full_report)

            for chunk in report_chunks:
                if app_env == "production":
                    logger.info(
                        f"\n--- Відправка зведеного звіту адміністраторам ({', '.join(map(str, admin_chat_ids))}) ---"
                    )
                    sent_msgs = await send_notification(
                        bot=bot,
                        chat_ids=admin_chat_ids,  # Передаем список ID напрямую
                        text=chunk,
                    )
                    # Запланувати видалення через 30 хвилин
                    from ..utils import schedule_message_deletion
                    for msg in sent_msgs:
                        await schedule_message_deletion(chat_id=msg.chat.id, message_id=msg.message_id, delay_minutes=30)
                    logger.info("✅ Частину зведеного звіту успішно відправлено.")
                else:
                    logger.info(
                        f"\n--- [DEV] Зведений звіт для адміністраторів ({', '.join(map(str, admin_chat_ids))}) ---"
                    )
                    logger.info(chunk)
                    logger.info(f"--- [DEV] Кінець зведеного звіту ---\n")
        except Exception as e:
            logger.info(f"!!! Помилка при відправці зведеного звіту адміністратору: {e}")
