# app/data_loader.py
import asyncio
import uuid
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
    moved_raw: bytes,
    ordered_raw: bytes,
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
    df_moved_raw = await run_in_threadpool(process_moved_raw_data, moved_raw)
    df_ordered_raw = await run_in_threadpool(process_ordered_raw_data, ordered_raw)

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

    if not df_moved_raw.empty and not df_ordered_raw.empty:
        merged_df = pd.merge(
            df_ordered_raw,
            df_moved_raw,
            on=["Заявка на відвантаження", "Номенклатура"],
            how="outer",
            suffixes=("_ordered", "_moved"),
        )

        cols_to_coalesce = [
            c.replace("_ordered", "")
            for c in merged_df.columns
            if c.endswith("_ordered")
        ]

        for col_base in cols_to_coalesce:
            col_ordered = f"{col_base}_ordered"
            col_moved = f"{col_base}_moved"

            if col_ordered in merged_df.columns and col_moved in merged_df.columns:
                merged_df[col_base] = np.where(
                    pd.notna(merged_df[col_ordered]) & (merged_df[col_ordered] != ""),
                    merged_df[col_ordered],
                    merged_df[col_moved],
                )
                merged_df.drop(columns=[col_ordered, col_moved], inplace=True)

        test_data = merged_df.iloc[2970:].copy()

        if "Примечание_перемещено" in test_data.columns:
            test_data = test_data.drop(columns=["Примечание_перемещено"])

        test_data["Товар"] = test_data.apply(
            lambda row: f"{row.get('Номенклатура', '')} {row.get('Ознака партії', '')} {row.get('Сезон закупівлі', '')}".strip(),
            axis=1,
        )

        test_data["Заказано"] = pd.to_numeric(test_data["Заказано"], errors="coerce")
        test_data["Перемещено"] = pd.to_numeric(
            test_data["Перемещено"], errors="coerce"
        )
        test_data.dropna(subset=["Перемещено", "Партія номенклатури"], inplace=True)
        test_data = test_data[test_data["Партія номенклатури"] != ""].copy()
        test_data["Перемещено"] = test_data["Перемещено"].astype(int)

        # --- Этап 3: Автоматическая обработка и сопоставление ---
        matched_list = []
        leftovers = {}
        all_requests = test_data["Заявка на відвантаження"].dropna().unique()

        for request_id in all_requests:
            request_df = test_data[
                test_data["Заявка на відвантаження"] == request_id
            ].copy()
            if request_df.empty:
                continue

            total_ordered = request_df.groupby("Товар")["Заказано"].first().sum()
            total_moved = request_df["Перемещено"].sum()
            product = request_df["Товар"].iloc[0]
            current_moved = request_df.copy()
            current_notes = pd.DataFrame(columns=["Договор", "Количество_в_примечании"])
            note_text = ""

            if "Примечание_заказано" in current_moved.columns and not pd.isna(
                current_moved["Примечание_заказано"].iloc[0]
            ):
                note_text = current_moved["Примечание_заказано"].iloc[0]
                note_matches = re.findall(r"([А-Я]{2}-\d{8})-(\d+)", str(note_text))
                if note_matches:
                    current_notes = pd.DataFrame(
                        note_matches, columns=["Договор", "Количество_в_примечании"]
                    )
                    current_notes["Количество_в_примечании"] = pd.to_numeric(
                        current_notes["Количество_в_примечании"]
                    )

            if not current_moved.empty and not current_notes.empty:
                moved_counts = current_moved["Перемещено"].value_counts()
                notes_counts = current_notes["Количество_в_примечании"].value_counts()
                unique_qtys = moved_counts[(moved_counts == 1)].index.intersection(
                    notes_counts[(notes_counts == 1)].index
                )

                if not unique_qtys.empty:
                    unique_moved = current_moved[
                        current_moved["Перемещено"].isin(unique_qtys)
                    ]
                    unique_notes = current_notes[
                        current_notes["Количество_в_примечании"].isin(unique_qtys)
                    ]
                    matches_df = pd.merge(
                        unique_moved,
                        unique_notes,
                        left_on="Перемещено",
                        right_on="Количество_в_примечании",
                    )

                    for _, match_row in matches_df.iterrows():
                        matched_list.append(
                            {
                                "Договор": match_row["Договор"],
                                "Товар": product,
                                "Партия": match_row["Партія номенклатури"],
                                "Количество": match_row["Перемещено"],
                                "Вид деятельности": match_row["Вид діяльності"],
                                "Источник": "Автоматически",
                            }
                        )

                    current_moved = current_moved[
                        ~current_moved["Перемещено"].isin(unique_qtys)
                    ]
                    current_notes = current_notes[
                        ~current_notes["Количество_в_примечании"].isin(unique_qtys)
                    ]

            if not current_moved.empty and not current_notes.empty:
                if len(current_moved) == 1:
                    moved_qty = current_moved["Перемещено"].iloc[0]
                    notes_sum = current_notes["Количество_в_примечании"].sum()
                    if moved_qty == notes_sum:
                        moved_row_main = current_moved.iloc[0]
                        for _, note_row in current_notes.iterrows():
                            matched_list.append(
                                {
                                    "Договор": note_row["Договор"],
                                    "Товар": product,
                                    "Партия": moved_row_main["Партія номенклатури"],
                                    "Количество": note_row["Количество_в_примечании"],
                                    "Вид деятельности": moved_row_main[
                                        "Вид діяльності"
                                    ],
                                    "Источник": "Автоматически",
                                }
                            )
                        current_moved = pd.DataFrame(columns=current_moved.columns)
                        current_notes = pd.DataFrame(columns=current_notes.columns)

                if len(current_notes) == 1 and not current_moved.empty:
                    note_qty = current_notes["Количество_в_примечании"].iloc[0]
                    moved_sum = current_moved["Перемещено"].sum()
                    if note_qty == moved_sum:
                        note_row_main = current_notes.iloc[0]
                        for _, moved_row in current_moved.iterrows():
                            matched_list.append(
                                {
                                    "Договор": note_row_main["Договор"],
                                    "Товар": product,
                                    "Партия": moved_row["Партія номенклатури"],
                                    "Количество": moved_row["Перемещено"],
                                    "Вид деятельности": moved_row["Вид діяльності"],
                                    "Источник": "Автоматически",
                                }
                            )
                        current_moved = pd.DataFrame(columns=current_moved.columns)
                        current_notes = pd.DataFrame(columns=current_notes.columns)

            if not current_moved.empty and not current_notes.empty:
                leftovers[request_id] = {
                    "product": product,
                    "note_text": note_text,
                    "total_ordered": total_ordered,
                    "total_moved": total_moved,
                    "current_moved": [
                        dict(row, index=idx) for idx, row in current_moved.iterrows()
                    ],
                    "current_notes": [
                        dict(row, index=idx) for idx, row in current_notes.iterrows()
                    ],
                }

        session_id = "some_unique_session_id"

        leftovers = convert_numpy_types(leftovers)
        matched_list = convert_numpy_types(matched_list)
    else:
        print("DataFrame для OrderedMoved пуст, пропускаем вставку.")

    print("Все данные успешно сохранены в базу данных.")
