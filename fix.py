import sys

file_path = 'new_agri_bot_backend/nova_poshta.py'
with open(file_path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

new_lines = lines[:137]
new_lines.extend([
    '            return data_global\n',
    '        else:\n',
    '            logger.warning(f"All NP search methods returned empty for EDRPOU {edrpou}. Response: {data_global}")\n',
    '    \n',
    '    return data\n',
    '\n',
    '@router.get("/streets", dependencies=[Depends(check_not_guest)])\n',
    'async def get_streets(city_ref: str, q: str):\n',
    '    """Search for streets in a city for courier delivery"""\n',
    '    data = await call_np_api("Address", "searchSettlementStreets", {\n',
    '        "SettlementRef": city_ref,\n',
    '        "StreetName": q,\n',
    '        "Limit": 20\n',
    '    })\n',
    '    \n',
    '    if not data.get("success"):\n',
    '        return data\n',
    '        \n',
    '    results = []\n',
    '    if data.get("data") and len(data["data"]) > 0:\n',
    '        addresses = data["data"][0].get("Addresses", [])\n',
    '        for item in addresses:\n',
    '            results.append({\n',
    '                "description": item.get("SettlementStreetDescription"),\n',
    '                "ref": item.get("SettlementStreetRef"),\n',
    '                "street_type": item.get("StreetsTypeDescription")\n',
    '            })\n',
    '        \n',
    '    return {"success": True, "data": results}\n',
    '\n',
    '\n',
    '@router.get("/validate_ttn", dependencies=[Depends(check_not_guest)])\n',
    'async def validate_ttn(ttn: str):\n',
    '    """Validate TTN number"""\n',
    '    data = await call_np_api("TrackingDocument", "getStatusDocuments", {\n',
    '        "Documents": [\n',
    '            {\n',
    '                "DocumentNumber": ttn,\n',
    '                "Phone": ""\n',
    '            }\n',
    '        ]\n',
    '    })\n',
    '    \n',
    '    if not data.get("success"):\n',
    '        return data\n',
    '        \n',
    '    results = data.get("data", [])\n',
    '    if not results:\n',
    '        return {"success": False, "errors": ["ТТН не знайдено"], "data": []}\n',
    '        \n',
    '    status_code = results[0].get("StatusCode")\n',
    '    if status_code == "3" or status_code == 3:\n',
    '        return {"success": False, "errors": ["ТТН не знайдено (Статус 3)"], "data": []}\n',
    '        \n',
    '    return {"success": True, "data": results[0]}\n'
])

with open(file_path, 'w', encoding='utf-8') as f:
    f.writelines(new_lines)
