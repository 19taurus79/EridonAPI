# app/data_loader.py
import asyncio
import uuid
import json
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import numpy as np
import re
from typing import Dict, Any, Tuple, List

# from piccolo_conf import DB

# Импорты моделей Piccolo ORM
from .tables import (
    AvailableStock,
    Remains,
    Submissions,
    Payment,
    MovedData,
    ProductGuide,
    FreeStock,
)

# Импорты функций обработки данных
from .data_processing import (
    process_av_stock,
    process_remains_reg,
    process_submissions,
    process_payment,
    process_moved_data,
    process_free_stock,
    process_moved_raw_data,
    process_ordered_raw_data,
)

# Пул потоков для выполнения синхронных операций Pandas
executor = ThreadPoolExecutor(max_workers=4)


async def run_in_threadpool(func, *args):
    """Обертка для запуска синхронных функций в отдельном потоке."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(executor, func, *args)


# --- Вспомогательная функция для конвертации типов ---


def convert_numpy_types(data):
    """
    Рекурсивно обходит вложенные словари и списки, преобразуя типы данных NumPy
    (например, numpy.int64) в стандартные типы Python (int, float).
    Это необходимо, чтобы Pydantic мог корректно сериализовать данные в JSON.
    """
    if isinstance(data, dict):
        return {k: convert_numpy_types(v) for k, v in data.items()}
    if isinstance(data, list):
        return [convert_numpy_types(i) for i in data]
    if isinstance(data, np.integer):
        return int(data)
    if isinstance(data, np.floating):
        return float(data)
    if isinstance(data, np.ndarray):
        return data.tolist()
    return data


async def save_processed_data_to_db(
    av_stock_content: bytes,
    remains_content: bytes,
    submissions_content: bytes,
    payment_content: bytes,
    moved_content: bytes,
    free_stock_content: bytes,
    manual_matches_json: str = None,
):
    """
    Асинхронная функция для обработки и сохранения данных в базу данных.
    Оркестрирует вызовы синхронных функций обработки и асинхронных операций с БД.
    """
    print("Начало обработки и сохранения данных...")

    # 1. Запуск обработки Excel-файлов в отдельном потове
    df_av_stock = await run_in_threadpool(process_av_stock, av_stock_content)
    df_remains = await run_in_threadpool(process_remains_reg, remains_content)
    df_submissions = await run_in_threadpool(process_submissions, submissions_content)
    df_payment = await run_in_threadpool(process_payment, payment_content)
    df_moved = await run_in_threadpool(process_moved_data, moved_content)
    df_free_stock = await run_in_threadpool(process_free_stock, free_stock_content)

    # --- ГЛОБАЛЬНАЯ НОРМАЛИЗАЦИЯ: Очищаем 'product' во всех DataFrame ---
    df_av_stock["product"] = df_av_stock["product"].str.strip()
    df_remains["product"] = df_remains["product"].str.strip()
    df_submissions["product"] = df_submissions["product"].str.strip()
    df_moved["product"] = df_moved["product"].str.strip()
    df_free_stock["product"] = df_free_stock["product"].str.strip()

    print("Данные Excel обработаны в DataFrame. Начинаем сохранение в БД...")

    # print("Старі дані з таблиць видалено. Починаємо вставку нових даних...")
    # 2.1 Создание справочника товаров
    av_stock_tmp = df_av_stock[["product", "line_of_business", "active_substance"]]
    remains_tmp = df_remains[["product", "line_of_business", "active_substance"]]
    submissions_tmp = df_submissions[
        ["product", "line_of_business", "active_ingredient"]
    ].rename(columns={"active_ingredient": "active_substance"})
    pr = pd.concat([av_stock_tmp, submissions_tmp, remains_tmp], ignore_index=True)
    product_guide = pr.drop_duplicates(["product"]).reset_index(drop=True)
    
    # --- НОРМАЛИЗАЦИЯ: Очищаем 'product' от лишних пробелов ---
    product_guide["product"] = product_guide["product"].str.strip()
    
    product_guide.insert(0, "id", product_guide.apply(lambda _: uuid.uuid4(), axis=1))

    # 3. Вставка новых данных из DataFrame в соответствующие таблицы Piccolo
    # Вам нужно будет преобразовать DataFrame в список словарей, где ключи -
    # это имена колонок в базе данных, а значения - данные.
    # Убедитесь, что имена колонок DataFrame соответствуют полям в вашей модели Piccolo.
    BATCH_SIZE = 1000
    if not product_guide.empty:
        await ProductGuide.delete(force=True).run()
        # await ProductGuide.delete(force=True).run(node="DB_2")
        records_product_guide = product_guide.to_dict(orient="records")
        product_guide_raw = [ProductGuide(**item) for item in records_product_guide]
        for i in range(0, len(product_guide_raw), BATCH_SIZE):
            batch = product_guide_raw[i : i + BATCH_SIZE]
            rows = list(batch)
            await ProductGuide.insert().add(*rows).run()

    if not df_av_stock.empty:
        df_av_stock = df_av_stock.drop("active_substance", axis=1)
        await AvailableStock.delete(force=True).run()
        # await AvailableStock.delete(force=True).run(node="DB_2")
        av_stock_data = df_av_stock.merge(
            product_guide, on="product", how="left", suffixes=("_av", "_guide")
        )
        av_stock_data = av_stock_data.drop(
            ["product", "line_of_business_guide", "active_substance"], axis=1
        )
        av_stock_data = av_stock_data.rename(columns={"id": "product"})
        av_stock_data = av_stock_data.rename(
            columns={"line_of_business_av": "line_of_business"}
        )

        records_av_stock = av_stock_data.to_dict(orient="records")
        av_stock_raw = [AvailableStock(**item) for item in records_av_stock]

        for i in range(0, len(av_stock_raw), BATCH_SIZE):
            batch = av_stock_raw[i : i + BATCH_SIZE]
            rows = list(batch)
            await AvailableStock.insert().add(*rows).run()
            # await AvailableStock.insert().add(*rows).run(node="DB_2")

        # await AvailableStock.insert().add(*av_stock_raw).run()
        print(f"Вставлено {len(records_av_stock)} записей в AvailableStock.")
    else:
        print("DataFrame для AvailableStock пуст, пропускаем вставку.")

    # Пример:
    if not df_remains.empty:
        await Remains.delete(force=True).run()
        # await Remains.delete(force=True).run(node="DB_2")
        remains_data = df_remains.merge(
            product_guide, on="product", how="left", suffixes=("_av", "_guide")
        )
        remains_data = remains_data.drop(
            ["product", "line_of_business_guide", "active_substance_guide"], axis=1
        )
        remains_data = remains_data.rename(columns={"id": "product"})
        remains_data = remains_data.rename(
            columns={"active_substance_av": "active_substance"}
        )
        remains_data = remains_data.rename(
            columns={"line_of_business_av": "line_of_business"}
        )
        records_remains = remains_data.to_dict(orient="records")
        remains_raw = [Remains(**item) for item in records_remains]
        for i in range(0, len(remains_raw), BATCH_SIZE):
            batch = remains_raw[i : i + BATCH_SIZE]
            rows = list(batch)
            await Remains.insert().add(*rows).run()
            # await Remains.insert().add(*rows).run(node="DB_2")
        # await Remains.insert(*[Remains(**d) for d in records_remains]).run()
        print(f"Вставлено {len(records_remains)} записей в Remains.")
    else:
        print("DataFrame для Remains пуст, пропускаем вставку.")

    if not df_submissions.empty:
        await Submissions.delete(force=True).run()
        # await Submissions.delete(force=True).run(node="DB_2")
        submissions_data = df_submissions.merge(
            product_guide, on="product", how="left", suffixes=("_av", "_guide")
        )
        submissions_data = submissions_data.drop(
            ["product", "line_of_business_guide", "active_substance"], axis=1
        )
        submissions_data = submissions_data.rename(columns={"id": "product"})
        submissions_data = submissions_data.rename(
            columns={"line_of_business_av": "line_of_business"}
        )
        records_submissions = submissions_data.to_dict(orient="records")
        submissions_raw = [Submissions(**item) for item in records_submissions]
        for i in range(0, len(submissions_raw), BATCH_SIZE):
            batch = submissions_raw[i : i + BATCH_SIZE]
            rows = list(batch)
            await Submissions.insert().add(*rows).run()
            # await Submissions.insert().add(*rows).run(node="DB_2")
        # await Submissions.insert(*[Submissions(**d) for d in records_submissions]).run()
        print(f"Вставлено {len(records_submissions)} записей в Submissions.")
    else:
        print("DataFrame для Submissions пуст, пропускаем вставку.")

    if not df_payment.empty:
        await Payment.delete(force=True).run()
        # await Payment.delete(force=True).run(node="DB_2")
        records_payment = df_payment.to_dict(orient="records")
        payment_raw = [Payment(**item) for item in records_payment]
        for i in range(0, len(payment_raw), BATCH_SIZE):
            batch = payment_raw[i : i + BATCH_SIZE]
            rows = list(batch)
            await Payment.insert().add(*rows).run()
            # await Payment.insert().add(*rows).run(node="DB_2")
        # await Payment.insert(*[Payment(**d) for d in records_payment]).run()
        print(f"Вставлено {len(records_payment)} записей в Payment.")
    else:
        print("DataFrame для Payment пуст, пропускаем вставку.")

    if not df_moved.empty:
        await MovedData.delete(force=True).run()
        # await MovedData.delete(force=True).run(node="DB_2")
        moved_data = df_moved.merge(
            product_guide, on="product", how="left", suffixes=("_av", "_guide")
        )
        moved_data["id"] = moved_data["id"].astype(str)
        moved_data = moved_data.drop(
            ["line_of_business_guide", "active_substance"], axis=1
        )
        moved_data = moved_data.rename(
            columns={"line_of_business_av": "line_of_business"}
        )
        moved_data = moved_data.rename(columns={"id": "product_id"})
        records_moved = moved_data.to_dict(orient="records")
        moved_raw = [MovedData(**item) for item in records_moved]
        for i in range(0, len(moved_raw), BATCH_SIZE):
            batch = moved_raw[i : i + BATCH_SIZE]
            rows = list(batch)
            await MovedData.insert().add(*rows).run()
            # await MovedData.insert().add(*rows).run(node="DB_2")
        # await MovedData.insert(*[MovedData(**d) for d in records_moved]).run()
        print(f"Вставлено {len(records_moved)} записей в MovedData.")
    else:
        print("DataFrame для MovedData пуст, пропускаем вставку.")

    if not df_free_stock.empty:
        await FreeStock.delete(force=True).run()
        free_stock_data = df_free_stock.merge(
            product_guide, on="product", how="left", suffixes=("_av", "_guide")
        )
        free_stock_data = free_stock_data.drop(
            ["product", "line_of_business_guide", "active_substance"], axis=1
        )
        free_stock_data = free_stock_data.rename(columns={"id_guide": "product"})
        free_stock_data = free_stock_data.rename(columns={"id_av": "id"})
        free_stock_data = free_stock_data.rename(
            columns={"line_of_business_av": "line_of_business"}
        )
        free_stock_data.dropna(subset=["product"], inplace=True)

        records_free_stock = free_stock_data.to_dict(orient="records")
        free_stock_raw = [FreeStock(**item) for item in records_free_stock]

        for i in range(0, len(free_stock_raw), BATCH_SIZE):
            batch = free_stock_raw[i : i + BATCH_SIZE]
            rows = list(batch)
            await FreeStock.insert().add(*rows).run()

        print(f"Вставлено {len(records_free_stock)} записей в FreeStock.")
    else:
        print("DataFrame для FreeStock пуст, пропускаем вставку.")

    # Создаем DataFrame из данных ручного сопоставления, если они переданы
    df_manual_matches = pd.DataFrame()
    if manual_matches_json:
        try:
            manual_matches_list = json.loads(manual_matches_json)
            if manual_matches_list:
                # Сначала создаем DataFrame
                df_manual_matches = pd.DataFrame(
                    manual_matches_list.get("matched_data")
                    or manual_matches_list.get("matched_list", [])
                )
                print(
                    f"Создан DataFrame 'df_manual_matches' из ручных сопоставлений, размер: {df_manual_matches.shape}."
                )
        except (json.JSONDecodeError, AttributeError) as e:
            print(f"Ошибка при парсинге JSON из manual_matches_json: {e}")

    # Теперь, когда df_manual_matches может быть заполнен, проверяем его
    if not df_manual_matches.empty:
        try:
            # 1. Удаляем ненужные колонки
            columns_to_delete = [
                "Номенклатура",
                "Ознака партії",
                "Сезон закупівлі",
                "Примечание_заказано",
                "Перемещено",
                "Источник",
            ]
            df_matches = df_manual_matches.drop(
                columns=columns_to_delete, errors="ignore"
            )

            # 2. Переименовываем колонки для соответствия таблице MovedData
            rename_map = {
                "Заявка на відвантаження": "order",
                "Заказано": "qt_order",
                "Рік договору": "period",  # Пример, возможно нужно будет скорректировать
                "Партія номенклатури": "party_sign",
                "Вид діяльності": "line_of_business",
                "Дата": "date",
                "Товар": "product",
                "Договор": "contract",
                "Количество": "qt_moved",
            }
            df_matches = df_matches.rename(columns=rename_map)

            # --- НОРМАЛИЗАЦИЯ: Очищаем 'product' от лишних пробелов перед сопоставлением ---
            df_matches["product"] = df_matches["product"].str.strip()

            # 3. Сопоставляем с product_guide для получения product_id
            # Создаем временный справочник для сопоставления
            product_id_map = product_guide.set_index("product")["id"]
            df_matches["product_id"] = df_matches["product"].map(product_id_map)

            print("DataFrame df_matches после обработки и сопоставления:")
            print(df_matches.head())

        except Exception as e:
            print(f"Ошибка при финальной обработке df_manual_matches: {e}")
    else:
        print("Данные для ручного сопоставления не предоставлены или DataFrame пуст.")

    print("Все данные успешно сохранены в базу данных.")
