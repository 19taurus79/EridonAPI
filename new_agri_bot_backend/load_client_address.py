import asyncio
import os
import sys

import pandas as pd

from new_agri_bot_backend.tables import ClientAddress
from new_agri_bot_backend.config import logger


async def load_client_address_data(excel_filepath: str):
    """
    Читает данные из Excel-файла, очищает их и загружает в таблицу ClientAddress.
    """
    logger.info(f"--- Начало загрузки данных из файла: {excel_filepath} ---")

    try:
        # --- 1. Чтение и подготовка данных ---

        df = pd.read_excel(excel_filepath)

        # Выбираем только те колонки, которые есть в нашей модели
        model_columns = [c._meta.name for c in ClientAddress._meta.columns]
        df = df[[col for col in model_columns if col in df.columns]]

        # Очищаем данные: убираем лишние пробелы, заменяем пустые значения на None
        for col in df.select_dtypes(include=["object"]).columns:
            df[col] = df[col].str.strip()
        df.replace({"": None, pd.NaT: None}, inplace=True)

        # --- ИСПРАВЛЕНИЕ: Заполняем пустые значения в обязательных полях ---
        # Если в колонке 'representative' есть пустые значения (None), заменяем их
        # на строку "Не вказано", чтобы удовлетворить not-null constraint в БД.
        df["representative"].fillna("Не вказано", inplace=True)
        df["phone1"].fillna("Не вказано", inplace=True)
        df["phone2"].fillna("Не вказано", inplace=True)
        df["commune"].fillna("Не вказано", inplace=True)

        # Преобразуем DataFrame в список словарей для загрузки
        records_to_insert = df.to_dict("records")

        if not records_to_insert:
            logger.warning("⚠️ В файле не найдено данных для загрузки.")
            return

        # --- 2. Загрузка данных в базу данных ---

        # Сначала полностью очищаем таблицу
        logger.info("--- Очистка таблицы ClientAddress... ---")
        await ClientAddress.delete(force=True).run()
        logger.info("--- Таблица успешно очищена. ---")

        # Создаем список экземпляров модели Piccolo
        models_to_insert = [ClientAddress(**row) for row in records_to_insert]

        # Выполняем вставку пакетами, чтобы избежать ограничений БД
        BATCH_SIZE = 1000
        logger.info(f"--- Начало загрузки {len(models_to_insert)} записей... ---")

        for i in range(0, len(models_to_insert), BATCH_SIZE):
            batch = models_to_insert[i : i + BATCH_SIZE]
            await ClientAddress.insert(*batch).run()
            logger.info(f"  -> Загружен пакет {i // BATCH_SIZE + 1}...")

        logger.info(f"✅ Успешно загружено {len(models_to_insert)} записей в ClientAddress.")

    except FileNotFoundError:
        logger.error(f"❌ Ошибка: Файл '{excel_filepath}' не найден.")
    except KeyError as e:
        logger.error(f"❌ Ошибка: В Excel-файле отсутствует необходимая колонка: {e}")
    except Exception as e:
        logger.error(f"❌ Произошла непредвиденная ошибка: {e}")


# ----------------------------------------------------

if __name__ == "__main__":
    # ⚠️ ЗАМЕНИТЕ ЭТУ СТРОКУ НА АКТУАЛЬНЫЙ ПУТЬ К ВАШЕМУ ФАЙЛУ
    EXCEL_FILE_PATH = "../Контрагенты(адреса).xlsx"

    # Добавляем путь к корневой папке проекта для импорта piccolo_conf
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
    from piccolo_conf import DB

    async def run_main():
        """
        Управляет соединением с БД и запускает основную функцию загрузки.
        """
        try:
            await DB.start_connection_pool()
            await load_client_address_data(EXCEL_FILE_PATH)
        finally:
            await DB.close_connection_pool()

    asyncio.run(run_main())
    logger.info("\n🏁 Загрузка завершена.")
