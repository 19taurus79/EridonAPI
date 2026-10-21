"""Unit tests for /data/orders_search and /data/national_stock_search endpoints."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from httpx import AsyncClient, ASGITransport

from new_agri_bot_backend.main import app
from new_agri_bot_backend.telegram_auth import get_current_telegram_user
from new_agri_bot_backend.tables import (
    Submissions,
    Payment,
    ProductGuide,
    Remains,
    FreeStock,
)


@pytest.fixture
def mock_auth_user():
    return {
        "telegram_id": 548019148,
        "username": "manager_test",
        "full_name_for_orders": "Іван Петренко",
        "is_allowed": True,
        "is_admin": False,
    }


def make_query_mock(return_data):
    m = MagicMock()
    m.where.return_value = m
    m.order_by.return_value = m
    m.limit.return_value = m
    m.group_by.return_value = m
    m.run = AsyncMock(return_value=return_data)
    return m


@pytest.mark.asyncio
async def test_orders_search_endpoint(mock_auth_user):
    """Verifies that /data/orders_search queries Submissions and enriches with Payment."""
    app.dependency_overrides[get_current_telegram_user] = lambda: mock_auth_user

    mock_submissions = [
        {
            "id": "sub-1",
            "contract_supplement": "ТЕ-00029971",
            "client": "ФГ Русін",
            "manager": "Гаража",
            "nomenclature": "Аміачна селітра марка Б",
            "line_of_business": "Мінеральні добрива",
            "shipping_warehouse": "Харківський підрозділ с-ще Коротич",
            "document_status": "затверджено",
            "delivery_status": "Так (автоматично)",
            "plan": 20.0,
            "fact": 0.0,
            "different": 20.0,
            "party_sign": "Партія 2025",
        }
    ]

    mock_payments = [
        {
            "contract_supplement": "ТЕ-00029971",
            "contract_type": "Короткостроковий кредит",
            "loan_percentage": 0.0,
            "planned_amount": 350000.0,
            "actual_payment_amount": 0.0,
        }
    ]

    try:
        sub_mock = make_query_mock(mock_submissions)
        pay_mock = make_query_mock(mock_payments)

        with patch.object(Submissions, "select", return_value=sub_mock), \
             patch.object(Payment, "select", return_value=pay_mock):

            async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
                resp = await ac.get(
                    "/data/orders_search?product=селітра&manager=Гаража",
                    headers={"Authorization": "Bearer mock_token"},
                )

                assert resp.status_code == 200
                data = resp.json()
                assert len(data) == 1
                assert data[0]["contract_supplement"] == "ТЕ-00029971"
                assert data[0]["manager"] == "Гаража"
                assert data[0]["nomenclature"] == "Аміачна селітра марка Б"
                assert data[0]["different"] == 20.0
                assert data[0]["contract_type"] == "Короткостроковий кредит"
    finally:
        app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_national_stock_search_endpoint(mock_auth_user):
    """Verifies that /data/national_stock_search unites Remains and FreeStock."""
    app.dependency_overrides[get_current_telegram_user] = lambda: mock_auth_user

    mock_prods = [
        {
            "id": "prod-roundup",
            "product": "Раундап Макс, РК",
            "line_of_business": "ЗЗР",
            "parent_element": "Гербіциди",
        }
    ]

    mock_remains = [
        {
            "product": "prod-roundup",
            "warehouse": 'Харківський підрозділ  ТОВ "Фірма Ерідон" с-ще Коротич',
            "buh": 100.0,
            "skl": 100.0,
            "party_sign": "Партія 2025-Р1",
        }
    ]

    mock_free_stock = [
        {
            "product": "prod-roundup",
            "division": "Дніпровський підрозділ",
            "warehouse": "Дніпровський склад",
            "free_qty": 150.0,
        },
        {
            "product": "prod-roundup",
            "division": "Київський підрозділ",
            "warehouse": "Центральний склад Київ",
            "free_qty": 300.0,
        },
    ]

    mock_orders = [
        {"product_id": "prod-roundup", "orders_q": 20.0}
    ]

    try:
        pg_mock = make_query_mock(mock_prods)
        rem_mock = make_query_mock(mock_remains)
        fs_mock = make_query_mock(mock_free_stock)
        sub_mock = make_query_mock(mock_orders)

        with patch.object(ProductGuide, "select", return_value=pg_mock), \
             patch.object(Remains, "select", return_value=rem_mock), \
             patch.object(FreeStock, "select", return_value=fs_mock), \
             patch.object(Submissions, "select", return_value=sub_mock):

            async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
                resp = await ac.get(
                    "/data/national_stock_search?query=Раундап&free_only=true",
                    headers={"Authorization": "Bearer mock_token"},
                )

                assert resp.status_code == 200
                data = resp.json()
                assert len(data) == 1
                item = data[0]
                assert item["product"] == "Раундап Макс, РК"
                assert item["total_local_accounting_stock"] == 100.0
                assert item["total_reserved_orders"] == 20.0
                assert item["total_local_free_stock"] == 80.0  # 100 - 20
                assert item["total_regional_free_stock"] == 450.0  # 150 + 300
                assert item["total_national_free_stock"] == 530.0  # 80 + 450
                assert len(item["regional_stock"]) == 2
                assert item["regional_stock"][0]["division"] == "Дніпровський підрозділ"
                assert item["regional_stock"][0]["total_available"] == 150.0
    finally:
        app.dependency_overrides.clear()
