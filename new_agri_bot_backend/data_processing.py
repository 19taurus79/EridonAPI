# app/data_processing.py
import uuid

import pandas as pd
import io
from .config import valid_line_of_business, valid_warehouse  # Импорт из config.py


def read_excel_content(content: bytes, sheet_name=0) -> pd.DataFrame:
    """Вспомогательная функция для чтения содержимого Excel в DataFrame."""
    return pd.read_excel(io.BytesIO(content), sheet_name=sheet_name)


def process_submissions(content: bytes) -> pd.DataFrame:
    submissions = read_excel_content(content)
    submissions.drop(axis=0, labels=[0, 1, 2, 3, 4, 5, 6, 7], inplace=True)
    submissions.drop(axis=0, labels=submissions.tail(1).index, inplace=True)
    submissions.drop(
        axis=1, labels=["Unnamed: 1", "Unnamed: 2", "Unnamed: 6"], inplace=True
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
        "delivery_status",
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
        "delivery_status",
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
    submissions["contract_supplement"] = submissions["contract_supplement"].str.slice(
        23, 34
    )
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
    remains.drop(columns=["storage"], inplace=True)
    for col in ["buh", "skl"]:
        remains[col] = pd.to_numeric(remains[col], errors="coerce").fillna(0)
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
    # for col in ["qt_order", "qt_moved"]:
    #     moved[col] = pd.to_numeric(moved[col], errors="coerce").fillna(0)
    text_columns = [
        "order",
        "line_of_business",
        "product",
        "party_sign",
        "period",
        "contract",
        "qt_order",
        "qt_moved",
    ]
    for col in text_columns:
        moved[col] = moved[col].astype(str).fillna("")
    moved = moved.dropna(how="all")
    moved = moved.reset_index(drop=True)
    return moved
