# app/data_processing.py
import uuid

import pandas as pd
import numpy as np
import io
from .config import valid_line_of_business, valid_warehouse  # Импорт из config.py

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
    submissions["product"] = submissions.apply(
        lambda row: f"{str(row['nomenclature']).rstrip()} {str(row['party_sign']).rstrip()} {str(row['buying_season']).rstrip()}",
        axis=1,
    )
    submissions.insert(loc=15, column="delivery_status", value="")
    # submissions["contract_supplement"] = submissions["contract_supplement"].str.slice(
    #     23, 34
    # )
    return submissions


def process_av_stock(content: bytes) -> pd.DataFrame:
    av_stock = read_excel_content(content)
    av_stock.drop(axis=0, labels=[0, 1, 2, 3, 4, 5, 6], inplace=True)
    av_stock.drop(
        axis=1, labels=["Unnamed: 1", "Unnamed: 2", "Unnamed: 4"], inplace=True
    )
    av_col_names = [
        "nomenclature",
        "party_sign",
        "buying_season",
        "division",
        "line_of_business",
        "active_substance",
        "available",
    ]
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
    av_stock["product"] = av_stock.apply(
        lambda row: f"{row['nomenclature'].rstrip()} {row['party_sign'].rstrip()} {row['buying_season'].rstrip()}",
        axis=1,
    )
    # av_stock.drop("active_substance", axis=1, inplace=True)
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
    remains["product"] = remains.apply(
        lambda row: f"{row['nomenclature'].rstrip()} {row['party_sign'].rstrip()} {row['buying_season'].rstrip()}",
        axis=1,
    )
    remains = remains.loc[remains["line_of_business"].isin(valid_line_of_business)]
    remains = remains.loc[remains["warehouse"].isin(valid_warehouse)]
    return remains


def process_payment(content: bytes) -> pd.DataFrame:
    payment = read_excel_content(content)
    payment.drop(axis=0, labels=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9], inplace=True)
    payment.drop(
        axis=1, labels=["Unnamed: 1", "Unnamed: 2", "Unnamed: 7"], inplace=True
    )
    payment.drop(axis=0, labels=payment.tail(1).index, inplace=True)
    payment_col_name = [
        "contract_supplement",
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
    file = read_excel_content(content)
    file = file.loc[3:]
    file.columns = file.iloc[0]
    file = file[1:].reset_index(drop=True)
    new_columns = file.columns.tolist()
    new_columns[10] = "Свободно"
    new_columns[11] = "БухУч"
    new_columns[12] = "СкладУч"
    file.columns = new_columns
    file = file.loc[:, file.columns.notna()]
    file = file.iloc[2:].reset_index(drop=True)
    # Define the columns to convert to numeric
    numeric_cols = ["Свободно", "БухУч", "СкладУч"]

    # Convert specified columns to numeric and fill NaN with 0
    for col in numeric_cols:
        if col in file.columns:
            file[col] = (
                file[col].apply(lambda x: pd.to_numeric(x, errors="coerce")).fillna(0)
            )

    # Fill NaN with empty strings in other columns
    for col in file.columns:
        if col not in numeric_cols:
            file[col] = file[col].fillna("")
    file["product"] = (
        file.iloc[:, 0].astype(str)
        + " "
        + file.iloc[:, 1].astype(str)
        + " "
        + file.iloc[:, 2].astype(str)
    )
    # Get the names of the first three columns
    cols_to_drop = file.columns[:3].tolist()

    # Drop the columns
    file = file.drop(columns=cols_to_drop)
    # Get the current column names from the DataFrame
    current_cols = file.columns.tolist()

    # Define the new column names in the desired order
    new_cols = [
        "division",
        "warehouse",
        "date_in_co",
        "line_of_business",
        "free_qty",
        "buh_qty",
        "skl_qty",
        "product",
    ]

    # Check if the number of current columns matches the number of new names
    if len(current_cols) == len(new_cols):
        # Create a dictionary mapping current names to new names based on order
        rename_map = dict(zip(current_cols, new_cols))

        # Rename the columns
        file = file.rename(columns=rename_map)
        print("Столбцы успешно переименованы.")
    else:
        print(
            f"Ошибка: Количество текущих столбцов ({len(current_cols)}) не соответствует количеству новых имен ({len(new_cols)})."
        )
    # Get the current column names
    current_cols = file.columns.tolist()

    # Remove 'product' from the list
    current_cols.remove("product")

    # Create the new list with 'product' as the first column
    new_order = ["product"] + current_cols

    # Reindex the DataFrame with the new column order
    file = file[new_order]
    import uuid

    # Generate a list of unique UUIDs, one for each row
    uuids = [uuid.uuid4() for _ in range(len(file))]

    # Insert the 'id' column at the beginning of the DataFrame
    file.insert(0, "id", uuids)

    # Define the columns to check
    cols_to_check = ["free_qty", "buh_qty", "skl_qty"]

    # Filter out rows where all specified columns are 0
    # We keep rows where AT LEAST one of the specified columns is NOT 0
    file = file[(file[cols_to_check] != 0).any(axis=1)].reset_index(drop=True)
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
