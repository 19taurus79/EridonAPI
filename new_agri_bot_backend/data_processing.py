# app/data_processing.py
import uuid

import pandas as pd
import numpy as np
import io
from .config import valid_line_of_business, valid_warehouse, logger  # Импорт из config.py

# Опция для будущего поведения Pandas
pd.set_option("future.no_silent_downcasting", True)


def read_excel_content(content: bytes, sheet_name=0) -> pd.DataFrame:
    """Вспомогательная функция для чтения содержимого Excel в DataFrame."""
    return pd.read_excel(io.BytesIO(content), sheet_name=sheet_name, engine="openpyxl")


def process_submissions(content: bytes) -> pd.DataFrame:
    submissions = read_excel_content(content)
    submissions.drop(axis=0, labels=[0, 1, 2, 3, 4, 5, 6, 7], inplace=True)
    submissions.drop(axis=0, labels=submissions.tail(1).index, inplace=True)
    submissions.drop(
        axis=1,
        labels=["Unnamed: 1", "Unnamed: 2", "Unnamed: 6", "Unnamed: 7"],
        inplace=True,
    )
    submissions_col_names = [
        "division",
        "manager",
        "company_group",
        "client",
        "contract_supplement",
        "parent_element",
        "manufacturer",
        "active_ingredient",
        "nomenclature",
        "party_sign",
        "buying_season",
        "line_of_business",
        "period",
        "shipping_warehouse",
        "document_status",
        # "delivery_status",
        "shipping_address",
        "transport",
        "plan",
        "fact",
        "different",
    ]
    submissions.columns = submissions_col_names
    for col in ["plan", "fact", "different"]:
        submissions[col] = pd.to_numeric(submissions[col], errors="coerce").fillna(0)
    text_columns = [
        "division",
        "manager",
        "company_group",
        "client",
        "contract_supplement",
        "parent_element",
        "manufacturer",
        "active_ingredient",
        "nomenclature",
        "party_sign",
        "buying_season",
        "line_of_business",
        "shipping_warehouse",
        "document_status",
        # "delivery_status",
        "shipping_address",
        "transport",
        "period",
    ]
    for col in text_columns:
        submissions[col] = submissions[col].fillna("").astype(str)
    submissions.loc[
        submissions["party_sign"] == "Закупівля поточного сезону", "party_sign"
    ] = " "
    submissions["product"] = (
        submissions["nomenclature"].str.rstrip() + " " + 
        submissions["party_sign"].str.rstrip() + " " + 
        submissions["buying_season"].str.rstrip()
    ).str.strip()
    submissions.insert(loc=15, column="delivery_status", value="")
    # submissions["contract_supplement"] = submissions["contract_supplement"].str.slice(
    #     23, 34
    # )
    return submissions


def process_av_stock(content: bytes) -> pd.DataFrame:
    """
    Обработка файла 'Доступність товару підрозділи'.
    Новая структура (2026+):
      Строка 0: Заголовки (Номенклатура, Ознака партії, Сезон закупки, Склад.Підрозділ, Вид діяльності, Діюча речовина, Разом)
      Строка 1: Подзаголовок (Вільний залишок, б.о.)
      Строка 2: Excel ID (Excel000004153)
      Строка 3+: Данные
    """
    av_stock = read_excel_content(content)
    # Пропускаем строки 1 (подзаголовок) и 2 (Excel ID). 
    # Строка 0 ушла в заголовки DataFrame, поэтому индекс 2 — это 3-я строка данных.
    av_stock = av_stock.iloc[2:].reset_index(drop=True)

    # Убираем полностью пустые колонки если есть
    av_stock = av_stock.loc[:, av_stock.columns.notna()]
    av_stock = av_stock.dropna(axis=1, how="all")

    av_col_names = [
        "nomenclature",
        "party_sign",
        "buying_season",
        "division",
        "line_of_business",
        "active_substance",
        "available",
    ]

    if len(av_stock.columns) != len(av_col_names):
        logger.error(
            f"AV STOCK: Ожидалось {len(av_col_names)} столбцов, получено {len(av_stock.columns)}. "
            f"Текущие столбцы: {list(av_stock.columns)}"
        )

    av_stock.columns = av_col_names
    text_columns = [
        "nomenclature",
        "party_sign",
        "buying_season",
        "division",
        "line_of_business",
        "active_substance",
    ]
    for col in text_columns:
        av_stock[col] = av_stock[col].fillna("").astype(str)
    av_stock["available"] = pd.to_numeric(
        av_stock["available"], errors="coerce"
    ).fillna(0)
    av_stock["product"] = (
        av_stock["nomenclature"].str.rstrip() + " " + 
        av_stock["party_sign"].str.rstrip() + " " + 
        av_stock["buying_season"].str.rstrip()
    ).str.strip()
    return av_stock


def process_remains_reg(content: bytes) -> pd.DataFrame:
    remains = read_excel_content(content)
    remains.drop(axis=0, labels=[0, 1, 2, 3, 4], inplace=True)
    remains.drop(
        axis=1, labels=["Unnamed: 1", "Unnamed: 2", "Unnamed: 4"], inplace=True
    )
    remains.drop(axis=0, labels=remains.tail(1).index, inplace=True)
    remains_col_name = [
        "line_of_business",
        "warehouse",
        "parent_element",
        "nomenclature",
        "party_sign",
        "buying_season",
        "nomenclature_series",
        "mtn",
        "origin_country",
        "germination",
        "crop_year",
        "quantity_per_pallet",
        "active_substance",
        "certificate",
        "certificate_start_date",
        "certificate_end_date",
        "buh",
        "skl",
        "weight",
        "storage",
    ]
    remains.columns = remains_col_name
    # remains.drop(columns=["storage"], inplace=True)
    for col in ["buh", "skl", "storage"]:
        remains[col] = pd.to_numeric(remains[col], errors="coerce").fillna(0)
    remains["storage"] = np.where(
        remains["storage"] < 0, remains["storage"] * -1, remains["storage"]
    )
    text_columns = [
        "line_of_business",
        "warehouse",
        "parent_element",
        "nomenclature",
        "party_sign",
        "buying_season",
        "nomenclature_series",
        "mtn",
        "origin_country",
        "germination",
        "crop_year",
        "active_substance",
        "certificate",
        "certificate_start_date",
        "certificate_end_date",
        "weight",
        "quantity_per_pallet",
    ]
    for col in text_columns:
        remains[col] = remains[col].fillna("").astype(str)
    remains["product"] = (
        remains["nomenclature"].str.rstrip() + " " + 
        remains["party_sign"].str.rstrip() + " " + 
        remains["buying_season"].str.rstrip()
    ).str.strip()
    remains = remains.loc[remains["line_of_business"].isin(valid_line_of_business)]
    remains = remains.loc[remains["warehouse"].isin(valid_warehouse)]
    return remains


def process_payment(content: bytes) -> pd.DataFrame:
    payment = read_excel_content(content)
    payment.drop(axis=0, labels=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9], inplace=True)
    payment.drop(
        axis=1, labels=["Unnamed: 1", "Unnamed: 2", "Unnamed: 5"], inplace=True
    )  
    payment.drop(axis=0, labels=payment.tail(1).index, inplace=True)
    payment_col_name = [
        "contract_supplement",
        "client",
        "contract_type",
        "order_status",
        "prepayment_amount",
        "amount_of_credit",
        "prepayment_percentage",
        "loan_percentage",
        "planned_amount",
        "planned_amount_excluding_vat",
        "actual_sale_amount",
        "actual_payment_amount",
    ]
    payment.columns = payment_col_name
    numeric_columns = [
        "prepayment_amount",
        "amount_of_credit",
        "prepayment_percentage",
        "loan_percentage",
        "planned_amount",
        "planned_amount_excluding_vat",
        "actual_sale_amount",
        "actual_payment_amount",
    ]
    for col in numeric_columns:
        payment[col] = pd.to_numeric(payment[col], errors="coerce").fillna(0)
    # payment["client"] = payment["client"].astype(str).fillna("")
    payment["contract_supplement"] = (
        payment["contract_supplement"].astype(str).fillna("")
    )
    payment["contract_type"] = payment["contract_type"].astype(str).fillna("")
    return payment


def process_moved_data(content: bytes) -> pd.DataFrame:
    moved = read_excel_content(content, sheet_name="Данные")
    moved_col_names = [
        "order",
        "date",
        "line_of_business",
        "product",
        "qt_order",
        "qt_moved",
        "party_sign",
        "period",
        "contract",
    ]
    moved.columns = moved_col_names
    for col in ["qt_order", "qt_moved"]:
        moved[col] = pd.to_numeric(moved[col], errors="coerce").fillna(0)
    text_columns = [
        "order",
        "line_of_business",
        "product",
        "party_sign",
        "period",
        "contract",
    ]
    for col in text_columns:
        moved[col] = moved[col].astype(str).fillna("")
    moved = moved.dropna(how="all")
    moved = moved.reset_index(drop=True)
    return moved


def process_free_stock(content: bytes) -> pd.DataFrame:
    """
    Обработка файла 'Доступно'.
    Новая структура (2026+):
      Строка 0: Заголовки (Номенклатура, Ознака партії, Сезон закупки, Підрозділ, Склад,
                           Дата приходу товару на ЦО, Вид діяльності, Разом, -, -)
      Строка 1: Подзаголовки для числовых колонок (Вільний залишок, Залишок на складах, Залишок по складському обліку)
      Строка 2: Excel ID (Excel000004153)
      Строка 3+: Данные
    """
    file = read_excel_content(content)
    # Пропускаем строку 1 (подзаголовок) и 2 (Excel ID).
    # Строка 0 ушла в заголовки, поэтому индекс 2 — это 3-я строка данных.
    file = file.iloc[2:].reset_index(drop=True)

    # Убираем полностью пустые колонки если есть
    file = file.loc[:, file.columns.notna()]
    file = file.dropna(axis=1, how="all")

    # Новая структура: 10 колонок
    # Col[0]=Номенклатура, Col[1]=Ознака партії, Col[2]=Сезон закупки,
    # Col[3]=Підрозділ, Col[4]=Склад, Col[5]=Дата приходу,
    # Col[6]=Вид діяльності, Col[7]=Свободно, Col[8]=БухУч, Col[9]=СкладУч
    expected_cols = 10
    actual_cols = len(file.columns)

    if actual_cols != expected_cols:
        logger.error(
            f"FREE STOCK: Ожидалось {expected_cols} столбцов, получено {actual_cols}. "
            f"Текущие столбцы: {list(file.columns)}"
        )

    # Формируем product из первых 3 колонок (Номенклатура + Ознака партії + Сезон закупки)
    file["product"] = (
        file.iloc[:, 0].fillna("").astype(str).str.rstrip() + " " +
        file.iloc[:, 1].fillna("").astype(str).str.rstrip() + " " +
        file.iloc[:, 2].fillna("").astype(str).str.rstrip()
    ).str.strip()

    # Назначаем осмысленные имена
    col_mapping = {
        file.columns[0]: "_nomenclature",
        file.columns[1]: "_party_sign",
        file.columns[2]: "_buying_season",
        file.columns[3]: "division",
        file.columns[4]: "warehouse",
        file.columns[5]: "date_in_co",
        file.columns[6]: "line_of_business",
        file.columns[7]: "free_qty",
        file.columns[8]: "buh_qty",
        file.columns[9]: "skl_qty",
    }
    file = file.rename(columns=col_mapping)

    # Удаляем исходные 3 колонки номенклатуры (они уже в product)
    file = file.drop(columns=["_nomenclature", "_party_sign", "_buying_season"])

    # Числовые колонки
    numeric_cols = ["free_qty", "buh_qty", "skl_qty"]
    for col in numeric_cols:
        if col in file.columns:
            file[col] = pd.to_numeric(file[col], errors="coerce").fillna(0)

    # Текстовые колонки
    text_cols = ["division", "warehouse", "date_in_co", "line_of_business"]
    for col in text_cols:
        if col in file.columns:
            file[col] = file[col].fillna("").astype(str)

    # Ставим product первым столбцом
    cols_order = ["product"] + [c for c in file.columns if c != "product"]
    file = file[cols_order]

    # Генерируем UUID для каждой строки
    uuids = [uuid.uuid4() for _ in range(len(file))]
    file.insert(0, "id", uuids)

    # Фильтруем строки, где все числовые колонки == 0
    cols_to_check = ["free_qty", "buh_qty", "skl_qty"]
    file = file[(file[cols_to_check] != 0).any(axis=1)].reset_index(drop=True)

    logger.info(f"FREE STOCK: Обработано {len(file)} строк.")
    return file


def process_moved_raw_data(content: bytes) -> pd.DataFrame:
    moved = read_excel_content(content)
    moved = moved.drop(moved.index[0:3], axis=0)
    moved.columns = moved.iloc[0]
    moved = moved[1:].reset_index(drop=True)
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
    moved["Заявка на відвантаження"] = (
        moved["Заявка на відвантаження"].astype(str).str.extract(r"([А-Я]{2}-\d{8})")
    )
    return moved


def process_ordered_raw_data(content: bytes) -> pd.DataFrame:
    ordered = read_excel_content(content)
    ordered = ordered.drop(ordered.index[0:3], axis=0)
    ordered.columns = ordered.iloc[0]
    ordered = ordered[1:].reset_index(drop=True)
    ordered = ordered.dropna(axis=1, how="all").fillna("")
    ordered.columns = [
        f"{col}_{i}" if ordered.columns.duplicated()[i] else col
        for i, col in enumerate(ordered.columns)
    ]
    ordered = ordered.rename(
        columns={"Примечание_8": "Примечание_заказано", "Кількість": "Заказано"}
    )
    cols_to_drop_ordered = [
        col for col in ["Примечание", "Рік договору"] if col in ordered.columns
    ]
    ordered = ordered.drop(columns=cols_to_drop_ordered)
    ordered["Заявка на відвантаження"] = (
        ordered["Заявка на відвантаження"].astype(str).str.extract(r"([А-Я]{2}-\d{8})")
    )
    return ordered
