# app/data_loader.py
import asyncio
import csv
import uuid
import json
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import numpy as np
import re
from typing import Dict, Any, Tuple, List, Optional

from piccolo.query import Insert

from .config import bot
from .services.ordered_moved_notifications import notifications

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
    AddressGuide,
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
    # moved_content: bytes,
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
    # df_moved = await run_in_threadpool(process_moved_data, moved_content)
    df_free_stock = await run_in_threadpool(process_free_stock, free_stock_content)

    # --- ГЛОБАЛЬНАЯ НОРМАЛИЗАЦИЯ: Очищаем 'product' во всех DataFrame ---
    # --- ИСПРАВЛЕНИЕ: Преобразуем поля количества в float для df_free_stock ---
    for col in ["free_qty", "buh_qty", "skl_qty"]:
        if col in df_free_stock.columns:
            # Преобразуем в числовой тип, нечисловые значения станут NaN, затем заменяем NaN на 0.0
            df_free_stock[col] = pd.to_numeric(df_free_stock[col], errors='coerce').fillna(0.0).astype(float)
    # ---------------------------------------------------------------------

    df_av_stock["product"] = df_av_stock["product"].str.strip()
    df_remains["product"] = df_remains["product"].str.strip()
    df_submissions["product"] = df_submissions["product"].str.strip()
    # df_moved["product"] = df_moved["product"].str.strip()
    df_free_stock["product"] = df_free_stock["product"].str.strip()

    print("Данные Excel обработаны в DataFrame. Начинаем сохранение в БД...")

    # 2.1 Создание справочника товаров
    av_stock_tmp = df_av_stock[["product", "line_of_business", "active_substance"]]
    remains_tmp = df_remains[["product", "line_of_business", "active_substance"]]
    submissions_tmp = df_submissions[
        ["product", "line_of_business", "active_ingredient"]
    ].rename(columns={"active_ingredient": "active_substance"})
    pr = pd.concat([av_stock_tmp, submissions_tmp, remains_tmp], ignore_index=True)
    product_guide = pr.drop_duplicates(["product"]).reset_index(drop=True)

    product_guide["product"] = product_guide["product"].str.strip()
    product_guide.insert(0, "id", product_guide.apply(lambda _: uuid.uuid4(), axis=1))

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
            df_submissions["client"] = df_submissions["client"].str.strip()
            df_payment["client"] = df_payment["client"].str.strip()
            df_payment["contract_supplement"] = (
                df_payment["contract_supplement"].str.strip()
            )
            df_submissions["contract_supplement"] = (
                df_submissions["contract_supplement"].str.strip()
            )
            submissions_data = pd.merge(
                submissions_data,
                df_payment[["client", "contract_supplement", "order_status"]],
                how="left",
                on=["client", "contract_supplement"],
            )
            submissions_data.drop(columns="delivery_status", inplace=True)
            submissions_data = submissions_data.rename(
                columns={"order_status": "delivery_status"}
            )

            submissions_data["delivery_status"] = submissions_data[
                "delivery_status"
            ].fillna("Ні")
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
            df_payment_for_db = df_payment
            records_payment = df_payment_for_db.to_dict(orient="records")
            payment_raw = [Payment(**item) for item in records_payment]
            for i in range(0, len(payment_raw), BATCH_SIZE):
                batch = payment_raw[i : i + BATCH_SIZE]
                await Payment.insert().add(*list(batch)).run()
            print(f"Вставлено {len(records_payment)} записей в Payment.")
        except Exception as e:
            print(f"!!! Ошибка при сохранении данных в Payment: {e}")
    else:
        print("DataFrame для Payment пуст, пропускаем вставку.")
        
    moved = await MovedData.select().run()
    df_moved = pd.DataFrame(moved)
    df_moved = pd.merge(
        product_guide[["product", "id"]],
        df_moved,
        on="product",
        suffixes=["guide", "moved"],
    )
    df_moved["product_id"] = df_moved["idguide"]
    df_moved["product_id"] = df_moved["product_id"].astype(str)
    df_moved = df_moved.drop(columns=["idguide", "idmoved"])
    if not df_moved.empty:
        try:
            await MovedData.delete(force=True).run()
            # moved_data = df_moved.merge(
            #     product_guide, on="product", how="left", suffixes=("_av", "_guide")
            # )
            # moved_data["id"] = moved_data["id"].astype(str)
            # moved_data = moved_data.drop(
            #     ["line_of_business_guide", "active_substance"], axis=1
            # )
            # moved_data = moved_data.rename(
            #     columns={"line_of_business_av": "line_of_business"}
            # )
            # moved_data = moved_data.rename(columns={"id": "product_id"})
            moved_data = df_moved.copy()
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

    df_manual_matches = pd.DataFrame()
    if manual_matches_json:
        try:
            manual_matches_list = json.loads(manual_matches_json)
            if manual_matches_list:
                df_manual_matches = pd.DataFrame(
                    manual_matches_list.get("matched_data")
                    or manual_matches_list.get("matched_list", [])
                )
                if "Дата" in df_manual_matches.columns:
                    df_manual_matches["Дата"] = pd.to_datetime(
                        df_manual_matches["Дата"], errors="coerce"
                    )
                print(
                    f"Создан DataFrame 'df_manual_matches' из ручных сопоставлений, размер: {df_manual_matches.shape}."
                )
        except (json.JSONDecodeError, AttributeError) as e:
            print(f"Ошибка при парсинге JSON из manual_matches_json: {e}")

    if not df_manual_matches.empty:
        try:
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

            rename_map = {
                "Заявка на відвантаження": "order",
                "Заказано": "qt_order",
                "Рік договору": "period",
                "Партія номенклатури": "party_sign",
                "Вид діяльності": "line_of_business",
                "Дата": "date",
                "Товар": "product",
                "Договор": "contract",
                "Количество": "qt_moved",
            }
            df_matches = df_matches.rename(columns=rename_map)

            for col in ["product", "contract", "period"]:
                if col in df_matches.columns:
                    df_matches[col] = df_matches[col].astype(str).str.strip()

            df_matches["product"] = df_matches["product"].str.strip()

            product_id_map = product_guide.set_index("product")["id"]
            df_matches["product_id"] = df_matches["product"].map(product_id_map)

            # --- ОБНОВЛЕННАЯ ПРОВЕРКА ДУБЛИКАТОВ ---
            # Используем расширенный набор полей для точной идентификации записи
            existing_moved_data_list = await MovedData.select(
                MovedData.product_id, 
                MovedData.order, 
                MovedData.party_sign, 
                MovedData.qt_moved, 
                MovedData.date
            ).run()
            df_moved_from_db = pd.DataFrame(existing_moved_data_list)

            df_new_matches_to_add = pd.DataFrame()

            # Функция для создания уникального ключа (нормализация данных)
            def create_key(row):
                # Нормализация количества (100.0 -> "100")
                q = row.get("qt_moved")
                try:
                    q_val = float(q)
                    q_str = f"{q_val:g}"
                except (ValueError, TypeError):
                    q_str = str(q) if pd.notna(q) else ""
                
                # Нормализация даты
                d = row.get("date")
                d_str = str(d).split(" ")[0] if pd.notna(d) else ""
                
                return f"{row.get('product_id')}_{row.get('order')}_{row.get('party_sign')}_{q_str}_{d_str}"

            if not df_moved_from_db.empty:
                # Генерируем ключи для существующих записей
                df_moved_from_db["composite_key"] = df_moved_from_db.apply(create_key, axis=1)
                
                # Генерируем ключи для новых записей
                # Важно: df_matches уже содержит переименованные колонки (order, qt_moved и т.д.)
                df_matches["composite_key"] = df_matches.apply(create_key, axis=1)

                existing_keys = set(df_moved_from_db["composite_key"])
                df_new_matches_to_add = df_matches[
                    ~df_matches["composite_key"].isin(existing_keys)
                ].copy()
                df_new_matches_to_add.drop(columns=["composite_key"], inplace=True)
            else:
                df_new_matches_to_add = df_matches.copy()

            if not df_new_matches_to_add.empty:
                print(
                    f"Найдено {len(df_new_matches_to_add)} новых записей для добавления в MovedData."
                )
                df_new_matches_to_add = df_new_matches_to_add.replace({np.nan: None})

                # --- ИСПРАВЛЕНИЕ ЗДЕСЬ ---
                if "product_id" in df_new_matches_to_add.columns:
                    df_new_matches_to_add["product_id"] = df_new_matches_to_add[
                        "product_id"
                    ].astype(str)

                cols_to_str = ["period", "party_sign"]
                for col in cols_to_str:
                    if col in df_new_matches_to_add.columns:
                        df_new_matches_to_add[col] = df_new_matches_to_add[col].apply(
                            lambda x: (
                                str(int(x))
                                if pd.notna(x) and isinstance(x, (int, float, np.integer, np.floating))
                                else (str(x) if pd.notna(x) else None)
                            )
                        )

                records_to_add = df_new_matches_to_add.to_dict(orient="records")

                valid_columns = MovedData._meta.columns
                cleaned_records = [
                    {k: v for k, v in rec.items() if k in valid_columns}
                    for rec in records_to_add
                ]

                await MovedData.insert(
                    *[MovedData(**rec) for rec in cleaned_records]
                ).run()
                print("Новые записи успешно добавлены в MovedData.")
                await notifications(bot=bot, frame=df_new_matches_to_add)
            else:
                print("Новых записей для добавления в MovedData не найдено.")

        except Exception as e:
            print(f"Ошибка при финальной обработке df_manual_matches: {e}")
    else:
        print("Данные для ручного сопоставления не предоставлены или DataFrame пуст.")

    print("Все данные успешно сохранены в базу данных.")
