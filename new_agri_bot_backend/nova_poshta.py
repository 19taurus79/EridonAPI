from fastapi import APIRouter, Depends, Query, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Any
import httpx
from .config import NP_API_KEY, logger
from .telegram_auth import check_not_guest

router = APIRouter(prefix="/nova-poshta", tags=["Nova Poshta"])

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

@router.get("/cities", dependencies=[Depends(check_not_guest)])
async def get_cities(q: str = Query(..., min_length=2)):
    """Search for settlements/cities by name"""
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

@router.get("/warehouses", dependencies=[Depends(check_not_guest)])
async def get_warehouses(city_ref: str, q: Optional[str] = None, type_ref: Optional[str] = None):
    """Get warehouses for a city using its CityRef."""
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

@router.get("/counterparty", dependencies=[Depends(check_not_guest)])
async def get_counterparty(edrpou: str):
    """Find counterparty (organization) by EDRPOU code"""
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

@router.get("/streets", dependencies=[Depends(check_not_guest)])
async def get_streets(city_ref: str, q: str):
    """Search for streets in a city for courier delivery"""
    data = await call_np_api("Address", "getStreet", {
        "CityRef": city_ref,
        "FindByString": q,
        "Limit": 20
    })
    
    if not data.get("success"):
        return data
        
    results = []
    for item in data.get("data", []):
        results.append({
            "description": item.get("Description"),
            "ref": item.get("Ref"),
            "street_type": item.get("StreetsType")
        })
        
    return {"success": True, "data": results}
