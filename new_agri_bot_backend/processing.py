import pandas as pd
import re
from typing import Dict, Any, Tuple, List
import numpy as np

# --- Вспомогательная функция для конвертации типов ---


def convert_numpy_types(data):
    """
    Рекурсивно обходит вложенные словари и списки, преобразуя типы данных NumPy
    (например, numpy.int64) в стандартные типы Python (int, float).
    Это необходимо, чтобы Pydantic/FastAPI могли корректно сериализовать
    результаты в формат JSON, который не понимает типы NumPy.
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


# --- Основная функция обработки ---

moved_file = "../Перемещено.xlsx"
ordered_file = "../Заказано.xlsx"


def process_uploaded_files(ordered_file, moved_file) -> Tuple[Dict, List]:
    """
    Основная функция, которая выполняет всю логику обработки файлов.
    Принимает два файла, сопоставляет данные и возвращает два объекта:

    1. leftovers: Словарь с данными, которые не удалось сопоставить автоматически.
    2. matched_list: Список сопоставленных записей.
    """

    # --- Этап 1: Загрузка и предварительная очистка данных ---
    ordered = pd.read_excel(ordered_file)
    ordered = ordered.drop(ordered.index[0:3], axis=0)
    ordered.columns = ordered.iloc[0]
    ordered = ordered[2:].reset_index(drop=True)
    ordered = ordered.dropna(axis=1, how="all").fillna("")
    ordered.columns = [
        f"{col}_{i}" if ordered.columns.duplicated()[i] else col
        for i, col in enumerate(ordered.columns)
    ]
    ordered = ordered.rename(
        columns={"Примечание_8": "Примечание_заказано", "Кількість": "Заказано"}
    )
    cols_to_drop_ordered = [col for col in ["Примечание"] if col in ordered.columns]
    ordered = ordered.drop(columns=cols_to_drop_ordered)
    ordered["Заказано"] = ordered["Заказано"].replace(["", " "], 0)

    if not ordered.empty:
        ordered = ordered.iloc[:-1]

    original_request_col_ordered = ordered["Заявка на відвантаження"].astype(str)
    ordered["Дата"] = pd.to_datetime(
        original_request_col_ordered.str.extract(r"(\d{2}\.\d{2}\.\d{4})")[0],
        format="%d.%m.%Y",
        errors="coerce",
    )
    ordered["Заявка на відвантаження"] = original_request_col_ordered.str.extract(
        r"([А-Я]{2}-\d{8})"
    )
    ordered["Товар"] = (
        ordered["Номенклатура"].astype(str)
        + " "
        + ordered["Ознака партії"].astype(str)
        + " "
        + ordered["Сезон закупівлі"].astype(str)
    )

    moved = pd.read_excel(moved_file)
    moved = moved.drop(moved.index[0:3], axis=0)
    moved.columns = moved.iloc[0]
    moved = moved[2:].reset_index(drop=True)
    moved = moved.dropna(axis=1, how="all").fillna("")
    moved.columns = [
        f"{col}_{i}" if moved.columns.duplicated()[i] else col
        for i, col in enumerate(moved.columns)
    ]
    moved = moved.rename(
        columns={"Примечание_8": "Примечание_перемещено", "Количество": "Перемещено"}
    )
    cols_to_drop_moved = [col for col in ["Примечание"] if col in moved.columns]
    moved = moved.drop(columns=cols_to_drop_moved)

    if not moved.empty:
        moved = moved.iloc[:-1]

    original_request_col_moved = moved["Заявка на відвантаження"].astype(str)
    moved["Дата"] = pd.to_datetime(
        original_request_col_moved.str.extract(r"(\d{2}\.\d{2}\.\d{4})")[0],
        format="%d.%m.%Y",
        errors="coerce",
    )
    moved["Заявка на відвантаження"] = original_request_col_moved.str.extract(
        r"([А-Я]{2}-\d{8})"
    )
    moved["Товар"] = (
        moved["Номенклатура"].astype(str)
        + " "
        + moved["Ознака партії"].astype(str)
        + " "
        + moved["Сезон закупівлі"].astype(str)
    )

    merged_df = pd.merge(
        ordered,
        moved,
        on=["Заявка на відвантаження", "Товар"],
        how="outer",
        suffixes=("_ordered", "_moved"),
    )

    cols_to_coalesce = [
        c.replace("_ordered", "") for c in merged_df.columns if c.endswith("_ordered")
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

    test_data = merged_df.copy()

    if "Примечание_перемещено" in test_data.columns:
        test_data = test_data.drop(columns=["Примечание_перемещено"])

    test_data["Заказано"] = pd.to_numeric(test_data["Заказано"], errors="coerce")
    test_data["Перемещено"] = pd.to_numeric(test_data["Перемещено"], errors="coerce")
    test_data.dropna(subset=["Перемещено", "Партія номенклатури"], inplace=True)
    test_data = test_data[test_data["Партія номенклатури"] != ""].copy()
    test_data["Перемещено"] = test_data["Перемещено"].astype(int)

    matched_list = []
    leftovers = {}
    all_requests = test_data["Заявка на відвантаження"].dropna().unique()

    for request_id in all_requests:
        try:
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
                # --- НОВЫЙ СЦЕНАРИЙ (ВЫСШИЙ ПРИОРИТЕТ): Если в примечании только один договор ---
                if len(current_notes) == 1:
                    note_row_main = current_notes.iloc[0]
                    for _, moved_row in current_moved.iterrows():
                        record = moved_row.to_dict()
                        record["Договор"] = note_row_main["Договор"]
                        record["Количество"] = moved_row["Перемещено"]
                        record["Источник"] = "Автоматически (один договор)"
                        matched_list.append(record)
                    # Так как все сопоставлено, очищаем и переходим к следующей заявке
                    current_moved = pd.DataFrame(columns=current_moved.columns)
                    current_notes = pd.DataFrame(columns=current_notes.columns)
                    continue  # Переходим к следующему request_id

                # --- Сценарий 1: Поиск однозначных совпадений по количеству ---
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
                        record = match_row.to_dict()
                        record["Количество"] = record["Перемещено"]
                        record["Источник"] = "Автоматически"
                        record.pop("Количество_в_примечании", None)
                        matched_list.append(record)

                    current_moved = current_moved[
                        ~current_moved["Перемещено"].isin(unique_qtys)
                    ]
                    current_notes = current_notes[
                        ~current_notes["Количество_в_примечании"].isin(unique_qtys)
                    ]

            if not current_moved.empty and not current_notes.empty:
                # --- Сценарий 2: Одно перемещение равно сумме примечаний ---
                if len(current_moved) == 1:
                    moved_qty = current_moved["Перемещено"].iloc[0]
                    notes_sum = current_notes["Количество_в_примечании"].sum()
                    if moved_qty == notes_sum:
                        moved_row_main = current_moved.iloc[0]
                        for _, note_row in current_notes.iterrows():
                            record = moved_row_main.to_dict()
                            record["Договор"] = note_row["Договор"]
                            record["Количество"] = note_row["Количество_в_примечании"]
                            record["Источник"] = "Автоматически"
                            matched_list.append(record)
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
        except Exception as e:
            print(f"!!! Ошибка при автоматической обработке заявки {request_id}: {e}")
            continue

    leftovers = convert_numpy_types(leftovers)
    matched_list = convert_numpy_types(matched_list)

    return leftovers, matched_list


if __name__ == "__main__":
    process_uploaded_files(ordered_file, moved_file)
