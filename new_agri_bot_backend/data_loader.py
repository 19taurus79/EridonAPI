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

    # --- НОРМАЛИЗАЦИЯ: Очищаем 'product' от лишних пробелов перед сопоставлением ---
    product_guide["product"] = product_guide["product"].str.strip()

    product_guide.insert(0, "id", product_guide.apply(lambda _: uuid.uuid4(), axis=1))

    # 3. Вставка новых данных из DataFrame в соответствующие таблицы Piccolo
    # Вам нужно будет преобразовать DataFrame в список словарей, где ключи -
    # это имена колонок в базе данных, а значения - данные.
    # Убедитесь, что имена колонок DataFrame соответствуют полям в вашей модели Piccolo.
    BATCH_SIZE = 1000
    if not product_guide.empty:
        try:
            await ProductGuide.delete(force=True).run()
            records_product_guide = product_guide.to_dict(orient="records")
            product_guide_raw = [ProductGuide(**item) for item in records_product_guide]
            for i in range(0, len(product_guide_raw), BATCH_SIZE):
                batch = product_guide_raw[i : i + BATCH_SIZE]
                await ProductGuide.insert().add(*list(batch)).run()
            print(f"Вставлено {len(records_product_guide)} записей в ProductGuide.")
        except Exception as e:
            print(f"!!! Ошибка при сохранении данных в ProductGuide: {e}")

    if not df_av_stock.empty:
        try:
            df_av_stock = df_av_stock.drop("active_substance", axis=1)
            await AvailableStock.delete(force=True).run()
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
                await AvailableStock.insert().add(*list(batch)).run()
            print(f"Вставлено {len(records_av_stock)} записей в AvailableStock.")
        except Exception as e:
            print(f"!!! Ошибка при сохранении данных в AvailableStock: {e}")
    else:
        print("DataFrame для AvailableStock пуст, пропускаем вставку.")

    # Пример:
    if not df_remains.empty:
        try:
            await Remains.delete(force=True).run()
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
                await Remains.insert().add(*list(batch)).run()
            print(f"Вставлено {len(records_remains)} записей в Remains.")
        except Exception as e:
            print(f"!!! Ошибка при сохранении данных в Remains: {e}")
    else:
        print("DataFrame для Remains пуст, пропускаем вставку.")

    if not df_submissions.empty:
        try:
            await Submissions.delete(force=True).run()
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
                await Submissions.insert().add(*list(batch)).run()
            print(f"Вставлено {len(records_submissions)} записей в Submissions.")
        except Exception as e:
            print(f"!!! Ошибка при сохранении данных в Submissions: {e}")
    else:
        print("DataFrame для Submissions пуст, пропускаем вставку.")

    if not df_payment.empty:
        try:
            await Payment.delete(force=True).run()
            records_payment = df_payment.to_dict(orient="records")
            payment_raw = [Payment(**item) for item in records_payment]
            for i in range(0, len(payment_raw), BATCH_SIZE):
                batch = payment_raw[i : i + BATCH_SIZE]
                await Payment.insert().add(*list(batch)).run()
            print(f"Вставлено {len(records_payment)} записей в Payment.")
        except Exception as e:
            print(f"!!! Ошибка при сохранении данных в Payment: {e}")
    else:
        print("DataFrame для Payment пуст, пропускаем вставку.")

    if not df_moved.empty:
        try:
            await MovedData.delete(force=True).run()
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
                await MovedData.insert().add(*list(batch)).run()
            print(f"Вставлено {len(records_moved)} записей в MovedData.")
        except Exception as e:
            print(f"!!! Ошибка при сохранении данных в MovedData: {e}")
    else:
        print("DataFrame для MovedData пуст, пропускаем вставку.")

    if not df_free_stock.empty:
        try:
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
                await FreeStock.insert().add(*list(batch)).run()

            print(f"Вставлено {len(records_free_stock)} записей в FreeStock.")
        except Exception as e:
            print(f"!!! Ошибка при сохранении данных в FreeStock: {e}")
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
                # --- ИСПРАВЛЕНИЕ ЗДЕСЬ: Явное преобразование колонки 'Дата' в datetime ---
                if "Дата" in df_manual_matches.columns:
                    df_manual_matches["Дата"] = pd.to_datetime(
                        df_manual_matches["Дата"], errors="coerce"
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

            # Приводим ключевые колонки к строковому типу для надежного сравнения
            for col in ["product", "contract", "period"]:
                if col in df_matches.columns:
                    df_matches[col] = df_matches[col].astype(str).str.strip()

            # --- НОРМАЛИЗАЦИЯ: Очищаем 'product' от лишних пробелов перед сопоставлением ---
            df_matches["product"] = df_matches["product"].str.strip()

            # 3. Сопоставляем с product_guide для получения product_id
            # Создаем временный справочник для сопоставления
            product_id_map = product_guide.set_index("product")["id"]
            df_matches["product_id"] = df_matches["product"].map(product_id_map)

            # 4. Заменяем product_id на id из product_guide
            # df_matches["product"] = df_matches["product_id"]
            # df_matches = df_matches.drop(columns=["product_id"])

            # --- СВЕРКА С БАЗОЙ ДАННЫХ ---
            # 1. Загружаем существующие данные из MovedData
            existing_moved_data_list = await MovedData.select(
                MovedData.product, MovedData.contract, MovedData.period
            ).run()
            df_moved_from_db = pd.DataFrame(existing_moved_data_list)

            df_new_matches_to_add = pd.DataFrame()

            if not df_moved_from_db.empty:
                # 2. Создаем композитный ключ для сравнения
                key_cols = ["product", "contract", "period"]
                for col in key_cols:
                    df_moved_from_db[col] = (
                        df_moved_from_db[col].astype(str).str.strip()
                    )

                df_matches["composite_key"] = df_matches[key_cols].apply(
                    lambda row: "_".join(row.values.astype(str)), axis=1
                )
                df_moved_from_db["composite_key"] = df_moved_from_db[key_cols].apply(
                    lambda row: "_".join(row.values.astype(str)), axis=1
                )

                # 3. Находим записи, которых нет в БД
                existing_keys = set(df_moved_from_db["composite_key"])
                df_new_matches_to_add = df_matches[
                    ~df_matches["composite_key"].isin(existing_keys)
                ].copy()
                df_new_matches_to_add.drop(columns=["composite_key"], inplace=True)

            else:
                # Если таблица в БД пуста, то все записи из df_matches являются новыми
                df_new_matches_to_add = df_matches.copy()

            # 4. Записываем в БД только новые записи
            if not df_new_matches_to_add.empty:
                print(
                    f"Найдено {len(df_new_matches_to_add)} новых записей для добавления в MovedData."
                )

                # Заменяем NaN на None перед записью
                df_new_matches_to_add = df_new_matches_to_add.replace({np.nan: None})

                # --- НАДЕЖНОЕ ПРЕОБРАЗОВАНИЕ ТИПОВ В СТРОКУ ---
                # Применяем функцию, которая корректно обрабатывает числа и None.
                # Это необходимо, т.к. колонка типа 'object' может содержать int/float.
                cols_to_str = [
                    "qt_order", "qt_moved", "product_id", "order", 
                    "party_sign", "period", "contract", "line_of_business"
                ]
                for col in cols_to_str:
                    if col in df_new_matches_to_add.columns:
                        df_new_matches_to_add[col] = df_new_matches_to_add[col].apply(
                            lambda x: str(x) if pd.notna(x) else None
                        )

                records_to_add = df_new_matches_to_add.to_dict(orient="records")

                # Убираем лишние ключи, которых нет в модели MovedData
                # valid_columns = MovedData._meta.column_names
                # cleaned_records = [{k: v for k, v in rec.items() if k in valid_columns} for rec in records_to_add]

                await MovedData.insert(
                    *[MovedData(**rec) for rec in records_to_add]
                ).run()
                print("Новые записи успешно добавлены в MovedData.")
            else:
                print("Новых записей для добавления в MovedData не найдено.")

            # Теперь df_new_matches_to_add доступен для вашей дальнейшей обработки

        except Exception as e:
            print(f"Ошибка при финальной обработке df_manual_matches: {e}")
    else:
        print("Данные для ручного сопоставления не предоставлены или DataFrame пуст.")

    print("Все данные успешно сохранены в базу данных.")
