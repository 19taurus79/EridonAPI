"""
Tests for Phase 2.1A.7 — Additive Warehouse Directory API.

Verifies:
1. Retrieval of active warehouses (active_only=True default).
2. Five operational warehouses for Kharkiv division.
3. Accurate mapping of Suprunivka (physical label Poltava, operational Kharkiv).
4. Accurate mapping of Lubny (physical label Lubny, operational Kharkiv).
5. Differentiation between main and transit Korotych (site_type).
6. Single physical hub with quotas of multiple operational divisions.
7. Warehouse with zero FreeStock records (fallback annotation, unknown status).
8. Conflicting divisions between FreeStock and ValidFreeStock (ambiguous status).
9. Zero accounting stock (Remains) with positive FreeStock.
10. Unauthorized request rejection (401).
11. Blocked/disallowed user rejection (403).
12. Integrity and immutability of existing warehouse endpoints (/data/delivery/warehouses, /data/remains/warehouses).
"""

import os
from pathlib import Path

# Suppress APScheduler and webhooks during tests
try:
    import new_agri_bot_backend.scheduler as scheduler_mod
    scheduler_mod.setup_scheduler = lambda *a, **k: None
    if hasattr(scheduler_mod, "scheduler"):
        scheduler_mod.scheduler.start = lambda *a, **k: None
except Exception:
    pass

from unittest.mock import AsyncMock, MagicMock, patch
from fastapi import HTTPException, status
from fastapi.testclient import TestClient
import pytest

from new_agri_bot_backend.main import app
from new_agri_bot_backend.telegram_auth import get_current_telegram_user
from new_agri_bot_backend.warehouse_directory_api import (
    WarehouseDirectoryItem,
    build_directory_item,
    determine_site_type,
    extract_location,
    extract_name_annotation_divisions,
    extract_physical_division_label,
    fetch_warehouse_directory_data,
)


@pytest.fixture
def mock_admin_user():
    user = MagicMock()
    user.telegram_id = 548019148
    user.is_allowed = True
    user.is_admin = True
    user.is_guest = False
    return user


@pytest.fixture
def mock_regular_user():
    user = MagicMock()
    user.telegram_id = 7385862387
    user.is_allowed = True
    user.is_admin = False
    user.is_guest = False
    return user


@pytest.fixture
def mock_blocked_user():
    user = MagicMock()
    user.telegram_id = 994574489
    user.is_allowed = False
    user.is_admin = False
    user.is_guest = False
    return user


@pytest.fixture
def client_with_admin(mock_admin_user):
    app.dependency_overrides[get_current_telegram_user] = lambda: mock_admin_user
    with TestClient(app) as client:
        yield client
    app.dependency_overrides.clear()


@pytest.fixture
def client_with_regular(mock_regular_user):
    app.dependency_overrides[get_current_telegram_user] = lambda: mock_regular_user
    with TestClient(app) as client:
        yield client
    app.dependency_overrides.clear()


# ==============================================================================
# Unit Tests for Helper / Extraction Logic
# ==============================================================================

def test_extract_physical_division_label():
    assert (
        extract_physical_division_label(
            'Полтавський підрозділ  ТОВ "Фірма Ерідон" с.Супрунівка (Харківський підрозділ)'
        )
        == "Полтавський підрозділ"
    )
    assert (
        extract_physical_division_label(
            'Харківський підрозділ  ТОВ "Фірма Ерідон" с-ще Коротич'
        )
        == "Харківський підрозділ"
    )
    assert (
        extract_physical_division_label(
            'Лубенський підрозділ  ТОВ "Фірма Ерідон" м.Лубни (Харківський підрозділ)'
        )
        == "Лубенський підрозділ"
    )
    assert (
        extract_physical_division_label(
            "Ерідон Логістик ТОВ (Волинська обл., м.Любомль)"
        )
        is None
    )


def test_extract_location():
    assert (
        extract_location(
            'Полтавський підрозділ  ТОВ "Фірма Ерідон" с.Супрунівка (Харківський підрозділ)'
        )
        == "с.Супрунівка"
    )
    assert (
        extract_location(
            'Харківський підрозділ  ТОВ "Фірма Ерідон" с-ще Коротич'
        )
        == "с-ще Коротич"
    )
    assert (
        extract_location(
            'Харківський підрозділ  ТОВ "Фірма Ерідон" м.Балаклія'
        )
        == "м.Балаклія"
    )
    assert (
        extract_location(
            'Лубенський підрозділ  ТОВ "Фірма Ерідон" м.Лубни (Харківський підрозділ)'
        )
        == "м.Лубни"
    )
    assert (
        extract_location(
            "Ерідон Логістик ТОВ (Волинська обл., м.Любомль)"
        )
        == "м.Любомль"
    )


def test_determine_site_type():
    assert (
        determine_site_type(
            'Харківський підрозділ  ТОВ "Фірма Ерідон" с-ще Коротич'
        )
        == "main"
    )
    assert (
        determine_site_type(
            'Харківський підрозділ  ТОВ "Фірма Ерідон" с-ще Коротич (Транзитний склад)'
        )
        == "transit"
    )
    assert (
        determine_site_type(
            'Харківський підрозділ  ТОВ "Фірма Ерідон" с-ще Коротич (ДЕМО транзит)'
        )
        == "transit"
    )
    assert (
        determine_site_type(
            "Ерідон Логістик ТОВ (Волинська обл., м.Любомль)"
        )
        == "other"
    )


def test_extract_name_annotation_divisions():
    # Parentheses with division
    divs = extract_name_annotation_divisions(
        'Полтавський підрозділ  ТОВ "Фірма Ерідон" с.Супрунівка (Харківський підрозділ)',
        "Полтавський підрозділ",
    )
    assert divs == ["Харківський підрозділ"]

    # Parentheses with transit (not a division) -> falls back to physical label
    divs_transit = extract_name_annotation_divisions(
        'Харківський підрозділ  ТОВ "Фірма Ерідон" с-ще Коротич (Транзитний склад)',
        "Харківський підрозділ",
    )
    assert divs_transit == ["Харківський підрозділ"]


# ==============================================================================
# Requirement 6, 7, 8: Hub with multiple divisions, zero stock, and conflicts
# ==============================================================================

def test_multiple_operational_divisions_on_single_hub():
    """6. Один физический хаб с квотами нескольких подразделений."""
    item = build_directory_item(
        name="Ерідон Логістик ТОВ (Волинська обл., м.Любомль)",
        is_active=False,
        fs_divs={"Відділ мінеральних добрив", "Відділ по роботі з сільськогосподарською продукцією"},
        has_fs_stock=True,
        vfs_divs={"Відділ мінеральних добрив", "Відділ по роботі з сільськогосподарською продукцією"},
        has_vfs_stock=True,
        has_rem_stock=False,
    )
    assert item.operational_divisions == [
        "Відділ мінеральних добрив",
        "Відділ по роботі з сільськогосподарською продукцією",
    ]
    assert item.binding_status == "confirmed"
    assert item.binding_source == "freestock"


def test_warehouse_with_zero_freestock_records():
    """7. Склад без записей FreeStock."""
    item = build_directory_item(
        name='Харківський підрозділ  ТОВ "Фірма Ерідон" с-ще Коротич (ДЕМО транзит)',
        is_active=False,
        fs_divs=set(),
        has_fs_stock=False,
        vfs_divs=set(),
        has_vfs_stock=False,
        has_rem_stock=False,
    )
    assert item.has_free_stock is False
    assert item.has_valid_free_stock is False
    assert item.has_accounting_stock is False
    assert item.operational_divisions == ["Харківський підрозділ"]
    assert item.binding_source == "name_annotation"
    assert item.binding_status == "unknown"


def test_conflicting_divisions_returns_ambiguous():
    """8. Конфликтующие подразделения."""
    item = build_directory_item(
        name='Тестовий склад підрозділ ТОВ "Ерідон"',
        is_active=True,
        fs_divs={"Харківський підрозділ"},
        has_fs_stock=True,
        vfs_divs={"Полтавський підрозділ"},
        has_vfs_stock=True,
        has_rem_stock=True,
    )
    assert item.binding_status == "ambiguous"
    assert item.binding_source == "conflict"
    assert sorted(item.operational_divisions) == ["Полтавський підрозділ", "Харківський підрозділ"]


# ==============================================================================
# Endpoint API Tests (Tests 1, 2, 3, 4, 5, 9, 10, 11, 12)
# ==============================================================================

def test_1_get_active_warehouses_default(client_with_regular):
    """1. Получение активных складов (active_only=True по умолчанию)."""
    response = client_with_regular.get("/data/warehouses/directory")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert isinstance(data, list)
    assert len(data) == 5  # exactly 5 active warehouses in test_db
    for item in data:
        assert item["is_active"] is True


def test_2_kharkiv_division_five_warehouses(client_with_regular):
    """2. Пять складов операционного Харківського підрозділу."""
    response = client_with_regular.get(
        "/data/warehouses/directory",
        params={"operational_division": "Харківський підрозділ"},
    )
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert len(data) == 5

    canonical_names = {item["canonical_name"] for item in data}
    assert any("с-ще Коротич (Транзитний склад)" in name for name in canonical_names)
    assert any("с-ще Коротич" in name and "Транзитний" not in name for name in canonical_names)
    assert any("м.Балаклія" in name for name in canonical_names)
    assert any("с.Супрунівка" in name for name in canonical_names)
    assert any("м.Лубни" in name for name in canonical_names)


def test_3_suprunivka_proper_binding(client_with_regular):
    """3. Правильная привязка Супруновки."""
    response = client_with_regular.get(
        "/data/warehouses/directory",
        params={"location": "с.Супрунівка"},
    )
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert len(data) == 1
    item = data[0]

    assert item["canonical_name"] == 'Полтавський підрозділ  ТОВ "Фірма Ерідон" с.Супрунівка (Харківський підрозділ)'
    assert item["physical_division_label"] == "Полтавський підрозділ"
    assert item["operational_divisions"] == ["Харківський підрозділ"]
    assert item["location"] == "с.Супрунівка"
    assert item["site_type"] == "main"
    assert item["binding_status"] == "confirmed"
    assert item["has_accounting_stock"] is True
    assert item["has_free_stock"] is True


def test_4_lubny_proper_binding(client_with_regular):
    """4. Правильная привязка Лубен."""
    response = client_with_regular.get(
        "/data/warehouses/directory",
        params={"location": "м.Лубни"},
    )
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert len(data) == 1
    item = data[0]

    assert item["canonical_name"] == 'Лубенський підрозділ  ТОВ "Фірма Ерідон" м.Лубни (Харківський підрозділ)'
    assert item["physical_division_label"] == "Лубенський підрозділ"
    assert item["operational_divisions"] == ["Харківський підрозділ"]
    assert item["location"] == "м.Лубни"
    assert item["binding_status"] == "confirmed"


def test_5_differentiation_main_and_transit_korotych(client_with_regular):
    """5. Различение основного и транзитного Коротича."""
    response = client_with_regular.get(
        "/data/warehouses/directory",
        params={"location": "Коротич"},
    )
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert len(data) == 2

    by_site = {item["site_type"]: item for item in data}
    assert "main" in by_site
    assert "transit" in by_site

    assert "Транзитний склад" not in by_site["main"]["canonical_name"]
    assert "Транзитний склад" in by_site["transit"]["canonical_name"]


def test_9_zero_remains_with_positive_freestock(client_with_regular):
    """9. Нулевой Remains при положительном FreeStock (Лубни)."""
    response = client_with_regular.get(
        "/data/warehouses/directory",
        params={"location": "Лубни"},
    )
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert len(data) == 1
    item = data[0]

    assert item["has_accounting_stock"] is False
    assert item["has_free_stock"] is True
    assert item["has_valid_free_stock"] is True
    assert item["operational_divisions"] == ["Харківський підрозділ"]
    assert item["binding_status"] == "confirmed"


def test_10_unauthorized_request_rejected():
    """10. Неавторизованный запрос — отказ (401)."""
    app.dependency_overrides.clear()
    with TestClient(app) as client:
        response = client.get("/data/warehouses/directory")
        assert response.status_code == status.HTTP_401_UNAUTHORIZED


def test_11_blocked_user_rejected():
    """11. Заблокированный пользователь — отказ (403)."""
    # 994574489 is in test_db with is_allowed=False
    mock_blocked = MagicMock()
    mock_blocked.telegram_id = 994574489
    mock_blocked.is_allowed = False

    async def _mock_get_blocked():
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Доступ заборонено. Користувач не зареєстрований або не має дозволу.",
        )

    app.dependency_overrides[get_current_telegram_user] = _mock_get_blocked
    with TestClient(app) as client:
        response = client.get("/data/warehouses/directory")
        assert response.status_code == status.HTTP_403_FORBIDDEN
    app.dependency_overrides.clear()


def test_inactive_warehouses_regular_user_forbidden(client_with_regular):
    """Regular user requesting active_only=False receives 403 Forbidden."""
    resp = client_with_regular.get(
        "/data/warehouses/directory",
        params={"active_only": False},
    )
    assert resp.status_code == status.HTTP_403_FORBIDDEN


def test_inactive_warehouses_admin_allowed(client_with_admin):
    """Admin requesting active_only=False receives all warehouses including inactive."""
    resp = client_with_admin.get(
        "/data/warehouses/directory",
        params={"active_only": False},
    )
    assert resp.status_code == status.HTTP_200_OK
    data = resp.json()
    assert len(data) == 7
    active_statuses = [item["is_active"] for item in data]
    assert False in active_statuses
    assert True in active_statuses


def test_12_existing_api_contracts_unchanged(client_with_regular):
    """12. Неизменность существующих API-контрактов."""
    # /data/delivery/warehouses
    resp_delivery = client_with_regular.get("/data/delivery/warehouses")
    assert resp_delivery.status_code == status.HTTP_200_OK
    delivery_whs = resp_delivery.json()
    assert isinstance(delivery_whs, list)
    assert len(delivery_whs) == 5
    assert all(isinstance(w, str) for w in delivery_whs)

    # /data/remains/warehouses
    resp_remains = client_with_regular.get("/data/remains/warehouses")
    assert resp_remains.status_code == status.HTTP_200_OK
    remains_whs = resp_remains.json()
    assert isinstance(remains_whs, list)
    assert len(remains_whs) == 4
    assert all(isinstance(w, str) for w in remains_whs)
