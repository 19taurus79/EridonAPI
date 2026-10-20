from fastapi import APIRouter, Depends, Query, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Any
import httpx
from .config import NP_API_KEY, logger
from .telegram_auth import check_not_guest

router = APIRouter(prefix="/nova-poshta", tags=["06. Нова Пошта (Nova Poshta)"])

NP_API_URL = "https://api.novaposhta.ua/v2.0/json/"

class NPRequest(BaseModel):
    apiKey: str
    modelName: str
    calledMethod: str
    methodProperties: dict

async def call_np_api(model: str, method: str, properties: dict) -> dict:
    if not NP_API_KEY:
        raise HTTPException(status_code=500, detail="Nova Poshta API key is not configured")
    
    payload = {
        "apiKey": NP_API_KEY,
        "modelName": model,
        "calledMethod": method,
        "methodProperties": properties
    }
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(NP_API_URL, json=payload)
            response.raise_for_status()
            data = response.json()
            if not data.get("success"):
                errors = data.get("errors", [])
                logger.error(f"Nova Poshta API error: {errors}")
                return {"success": False, "errors": errors, "data": []}
            return data
        except Exception as e:
            logger.error(f"Nova Poshta API connection error: {e}")
            raise HTTPException(status_code=502, detail=f"Error connecting to Nova Poshta API: {str(e)}")

@router.get(
    "/cities",
    dependencies=[Depends(check_not_guest)],
    summary="Пошук міст та населених пунктів (Нова Пошта)",
    description="Онлайн-пошук населених пунктів через AddressGeneral.searchSettlements за фрагментом назви для автодоповнення при оформленні доставки.",
)
async def get_cities(
    q: str = Query(..., min_length=2, description="Фрагмент назви населеного пункту (мін. 2 символи)"),
):
    """Пошук міст та населених пунктів через API Нової Пошти."""
    # Use searchSettlements for better autocomplete results
    data = await call_np_api("Address", "searchSettlements", {
        "CityName": q,
        "Limit": 20
    })
    
    if not data.get("success"):
        return data

    # Flatten the nested response for easier frontend consumption
    results = []
    for item in data.get("data", []):
        for address in item.get("Addresses", []):
            results.append({
                "present": address.get("Present"),
                "main_description": address.get("MainDescription"),
                "area": address.get("Area"),
                "region": address.get("Region"),
                "settlement_ref": address.get("DeliveryCity"), # This is actually CityRef needed for warehouses
                "ref": address.get("Ref")
            })
    
    return {"success": True, "data": results}

@router.get(
    "/warehouses",
    dependencies=[Depends(check_not_guest)],
    summary="Список відділень та поштоматів у місті (за CityRef)",
    description="Отримання списку вантажних/поштових відділень та поштоматів Нової Пошти у вибраному населеному пункті за його CityRef з можливістю текстового пошуку та фільтрації за типом відділення.",
)
async def get_warehouses(
    city_ref: str = Query(..., description="Унікальний ідентифікатор населеного пункту (CityRef/SettlementRef) у базі Нової Пошти"),
    q: Optional[str] = Query(None, description="Пошуковий рядок (номер відділення, вулиця або адреса)"),
    type_ref: Optional[str] = Query(None, description="Ідентифікатор типу відділення (поштове, вантажне, поштомат)"),
):
    """Список відділень та поштоматів у місті за CityRef."""
    properties = {
        "CityRef": city_ref,
        "Language": "UA",
        "Limit": 1000
    }
    if q:
        properties["FindByString"] = q
    if type_ref:
        properties["TypeOfWarehouseRef"] = type_ref
        
    data = await call_np_api("AddressGeneral", "getWarehouses", properties)
    
    if not data.get("success"):
        return data
        
    results = []
    for item in data.get("data", []):
        results.append({
            "description": item.get("Description"),
            "ref": item.get("Ref"),
            "number": item.get("Number"),
            "type_ref": item.get("TypeOfWarehouse"),
            "category": item.get("CategoryOfWarehouse"),
            "post_machine": item.get("PostMachineType") != "" or item.get("CategoryOfWarehouse") == "Postomat"
        })
    
    return {"success": True, "data": results}

@router.get(
    "/counterparty",
    dependencies=[Depends(check_not_guest)],
    summary="Пошук контрагента за ЄДРПОУ (Нова Пошта)",
    description="Перевірка наявності юридичної особи контрагента в довіднику контрагентів Нової Пошти за кодом ЄДРПОУ (шукає серед одержувачів і відправників) для автозаповнення реквізитів ТТН.",
)
async def get_counterparty(
    edrpou: str = Query(..., description="Код ЄДРПОУ підприємства контрагента (8 або 10 знаків)"),
):
    """Пошук контрагента за ЄДРПОУ в базі Нової Пошти."""
    logger.info(f"Searching for NP counterparty by EDRPOU: {edrpou}")
    
    # Try as Recipient first
    data = await call_np_api("Counterparty", "getCounterparties", {
        "CounterpartyProperty": "Recipient",
        "FindByString": edrpou
    })
    
    # If not found, try as Sender (sometimes they are registered as senders)
    if data.get("success") and not data.get("data"):
        logger.info(f"Not found as Recipient, trying as Sender for EDRPOU: {edrpou}")
        data_sender = await call_np_api("Counterparty", "getCounterparties", {
            "CounterpartyProperty": "Sender",
            "FindByString": edrpou
        })
        if data_sender.get("success") and data_sender.get("data"):
            return data_sender

    # If still not found, try without property
    if data.get("success") and not data.get("data"):
        logger.info(f"Not found as Recipient/Sender, trying without property for EDRPOU: {edrpou}")
        data_all = await call_np_api("Counterparty", "getCounterparties", {
            "FindByString": edrpou
        })
        if data_all.get("success") and data_all.get("data"):
            return data_all

    # Final attempt: global search method
    if data.get("success") and not data.get("data"):
        logger.info(f"Trying global search (getCounterpartyByEDRPOU) for: {edrpou}")
        data_global = await call_np_api("Counterparty", "getCounterpartyByEDRPOU", {
            "EDRPOU": edrpou
        })
        if data_global.get("success") and data_global.get("data"):
            return data_global
        else:
            logger.warning(f"All NP search methods returned empty for EDRPOU {edrpou}. Response: {data_global}")
    
    return data

@router.get(
    "/streets",
    dependencies=[Depends(check_not_guest)],
    summary="Пошук вулиць для адресної кур'єрської доставки",
    description="Пошук вулиць населеного пункту через Address.searchSettlementStreets за назвою для оформлення адресної кур'єрської доставки «до дверей».",
)
async def get_streets(
    city_ref: str = Query(..., description="Унікальний ідентифікатор населеного пункту (SettlementRef) у базі Нової Пошти"),
    q: str = Query(..., min_length=2, description="Фрагмент назви вулиці для пошуку"),
):
    """Пошук вулиць для адресної кур'єрської доставки."""
    data = await call_np_api("Address", "searchSettlementStreets", {
        "SettlementRef": city_ref,
        "StreetName": q,
        "Limit": 20
    })
    
    if not data.get("success"):
        return data
        
    results = []
    if data.get("data") and len(data["data"]) > 0:
        addresses = data["data"][0].get("Addresses", [])
        for item in addresses:
            results.append({
                "description": item.get("SettlementStreetDescription"),
                "ref": item.get("SettlementStreetRef"),
                "street_type": item.get("StreetsTypeDescription")
            })
        
    return {"success": True, "data": results}


@router.get(
    "/validate_ttn",
    dependencies=[Depends(check_not_guest)],
    summary="Перевірка валідності та статусу номера ТТН",
    description="Перевірка коректності 14-значного номера експрес-накладної та онлайн-запит актуального стану доставки вантажу через TrackingDocument.getStatusDocuments.",
)
async def validate_ttn(
    ttn: str = Query(..., description="14-значний номер електронної накладної Нової Пошти"),
):
    """Перевірка валідності та статусу номера ТТН Нової Пошти."""
    data = await call_np_api("TrackingDocument", "getStatusDocuments", {
        "Documents": [
            {
                "DocumentNumber": ttn,
                "Phone": ""
            }
        ]
    })
    
    if not data.get("success"):
        return data
        
    results = data.get("data", [])
    if not results:
        return {"success": False, "errors": ["ТТН не знайдено"], "data": []}
        
    status_code = results[0].get("StatusCode")
    if status_code == "3" or status_code == 3:
        return {"success": False, "errors": ["ТТН не знайдено (Статус 3)"], "data": []}
        
    return {"success": True, "data": results[0]}


@router.post(
    "/check-tracking",
    dependencies=[Depends(check_not_guest)],
    summary="Синхронізація статусів трекінгу ТТН (пакетне оновлення)",
    description="Службовий ендпоінт пакетного опитування активних ТТН. Знаходить усі відкриті доставки з ТТН у таблиці Deliveries, оновлює статуси та шле Telegram-сповіщення менеджерам при прибутті вантажу у відділення.",
)
async def trigger_check_np_deliveries():
    """Ручний або фоновий запуск пакетної перевірки статусів ТТН."""
    import asyncio
    from .services.np_tracking_service import check_np_deliveries_status
    asyncio.create_task(check_np_deliveries_status())
    return {"status": "ok", "message": "NP deliveries tracking check triggered in background."}
