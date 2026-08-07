@router.get("/validate_ttn", dependencies=[Depends(check_not_guest)])
async def validate_ttn(ttn: str):
    """Validate TTN number"""
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
    if status_code == '3' or status_code == 3:
        return {"success": False, "errors": ["ТТН не знайдено (Статус 3)"], "data": []}
        
    return {"success": True, "data": results[0]}
