import asyncio
import os
import sys
import re

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

        # --- ИСПРАВЛЕНИЕ: Автоматическое связывание и заполнение клиентов "ЕДО" ---
        logger.info("--- Проверка клиентов ЕДО ---")
        # Получаем старые данные до очистки таблицы на случай, если базового клиента больше нет в Excel
        existing_clients_db = await ClientAddress.select()
        existing_data_map = {client['client']: client for client in existing_clients_db}
        
        # Колонки для копирования (все кроме client, manager)
        cols_to_copy = [col for col in model_columns if col not in ['client', 'manager']]

        if 'client' in df.columns:
            edo_mask = df['client'].str.endswith('ЕДО', na=False)
            for idx, row in df[edo_mask].iterrows():
                client_name = row['client']
                # Убираем "ЕДО", " ЕДО", " - ЕДО" и т.д.
                base_client_name = re.sub(r'[\s\-]*ЕДО$', '', client_name).strip()
                
                base_data = None
                
                # 1. Ищем в текущем DataFrame
                base_df_rows = df[df['client'] == base_client_name]
                if not base_df_rows.empty:
                    base_data = base_df_rows.iloc[0].to_dict()
                # 2. Если не нашли в DataFrame, ищем в старой БД
                elif base_client_name in existing_data_map:
                    base_data = existing_data_map[base_client_name]
                    
                if base_data:
                    # Копируем данные только если они сейчас пусты в строке ЕДО
                    for col in cols_to_copy:
                        if col in df.columns and col in base_data:
                            val = row[col]
                            if pd.isna(val) or val == '' or val == 'Не вказано':
                                df.at[idx, col] = base_data[col]
                    logger.info(f"  -> Данные для '{client_name}' успешно скопированы из '{base_client_name}'")

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
