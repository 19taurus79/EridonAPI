"""
Модуль довідника складів (Phase 2.1A.7 — Additive Warehouse Directory API).

Надає безпечний read-only ендпоінт для отримання достовірних зв'язків
між складами та операційними підрозділами компанії.

Маршрут:
    GET /data/warehouses/directory
"""

import re
from typing import Any, Dict, List, Optional, Set
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from .tables import FreeStock, Remains, Users, ValidFreeStock, ValidWarehouseAdmin
from .telegram_auth import get_current_telegram_user


class WarehouseDirectoryItem(BaseModel):
    canonical_name: str = Field(description="Точне найменування з ValidWarehouseAdmin")
    physical_division_label: Optional[str] = Field(
        default=None,
        description="Підрозділ, зазначений на початку канонічної назви (строкова мітка)",
    )
    operational_divisions: List[str] = Field(
        default_factory=list,
        description="Операційні підрозділи з реальних зв'язків FreeStock / ValidFreeStock",
    )
    location: Optional[str] = Field(
        default=None,
        description="Населений пункт (якщо достовірно розпізнаний)",
    )
    site_type: str = Field(
        description="Тип майданчика: 'main' (основний) / 'transit' (транзитний) / 'other' (інший)",
    )
    is_active: bool = Field(description="Статус активності складу")
    has_accounting_stock: bool = Field(description="Наявність позитивного buh > 0 у Remains")
    has_free_stock: bool = Field(description="Наявність позитивного free_qty > 0 у FreeStock")
    has_valid_free_stock: bool = Field(
        description="Наявність позитивного free_qty > 0 у ValidFreeStock"
    )
    binding_source: str = Field(
        description="Джерело встановленого зв'язку: 'freestock' / 'valid_free_stock' / 'conflict' / 'name_annotation' / 'none'"
    )
    binding_status: str = Field(
        description="Статус операційної прив'язки: 'confirmed' / 'ambiguous' / 'unknown'"
    )


def extract_physical_division_label(name: str) -> Optional[str]:
    """Витягує назву підрозділу на початку канонічної назви (напр. 'Полтавський підрозділ').

    Якщо на початку рядка немає явного підрозділу — повертає None.
    """
    if not name:
        return None
    m = re.match(r"^(.*?підрозділ)\b", name.strip(), re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return None


def extract_location(name: str) -> Optional[str]:
    """Достовірно розпізнає населений пункт у назві складу:

    - с-ще Коротич, с-ще Добровеличківка
    - м.Балаклія, м.Лубни, м.Любомль, м.Суми
    - с.Супрунівка, с.Колоденка, с.Родниківка
    - ст.Богданівці
    """
    if not name:
        return None
    m = re.search(
        r"\b(с-ще\s+[А-ЯҐЄІЇа-яґєії’\'-]+|м\.[А-ЯҐЄІЇа-яґєії’\'-]+|с\.[А-ЯҐЄІЇа-яґєії’\'-]+|ст\.[А-ЯҐЄІЇа-яґєії’\'-]+)",
        name,
    )
    if m:
        return m.group(1).strip()
    return None


def determine_site_type(name: str) -> str:
    """Визначає тип майданчика: 'main' / 'transit' / 'other'.

    - 'transit': якщо в назві є 'транзит' (Транзитний склад, ДЕМО транзит тощо).
    - 'main': якщо це основний склад підрозділу (містить 'підрозділ' або 'основн').
    - 'other': інші склади (логістичні хаби, сторонні склади тощо).
    """
    if not name:
        return "other"
    n_lower = name.lower()
    if "транзит" in n_lower:
        return "transit"
    if "підрозділ" in n_lower or "основн" in n_lower:
        return "main"
    return "other"


def extract_name_annotation_divisions(
    name: str, physical_label: Optional[str]
) -> List[str]:
    """Витягує операційні підрозділи з назви як передбачуваний зв'язок:

    1. Спочатку перевіряє круглі дужки на кшталт (Харківський підрозділ),
       ігноруючи (Транзитний склад), (ДЕМО транзит) тощо.
    2. Якщо в дужках немає підрозділу, використовує physical_division_label як fallback.
    """
    if not name:
        return []
    parentheses_matches = re.findall(r"\(([^)]+)\)", name)
    found = []
    for content in parentheses_matches:
        c_clean = content.strip()
        c_lower = c_clean.lower()
        if "підрозділ" in c_lower and "транзит" not in c_lower:
            found.append(c_clean)
    if found:
        return sorted(list(set(found)))
    if physical_label:
        return [physical_label]
    return []


def build_directory_item(
    name: str,
    is_active: bool,
    fs_divs: Set[str],
    has_fs_stock: bool,
    vfs_divs: Set[str],
    has_vfs_stock: bool,
    has_rem_stock: bool,
) -> WarehouseDirectoryItem:
    """Чиста функція побудови елемента довідника за агрегованими даними."""
    phys_label = extract_physical_division_label(name)
    loc = extract_location(name)
    site_t = determine_site_type(name)

    # Визначення операційних підрозділів і статусу зв'язку
    if fs_divs and vfs_divs:
        if fs_divs == vfs_divs:
            op_divs = sorted(list(fs_divs))
            binding_src = "freestock"
            binding_st = "confirmed"
        else:
            op_divs = sorted(list(fs_divs | vfs_divs))
            binding_src = "conflict"
            binding_st = "ambiguous"
    elif fs_divs:
        op_divs = sorted(list(fs_divs))
        binding_src = "freestock"
        binding_st = "confirmed"
    elif vfs_divs:
        op_divs = sorted(list(vfs_divs))
        binding_src = "valid_free_stock"
        binding_st = "confirmed"
    else:
        annotated = extract_name_annotation_divisions(name, phys_label)
        if annotated:
            op_divs = sorted(list(annotated))
            binding_src = "name_annotation"
            binding_st = "unknown"
        else:
            op_divs = []
            binding_src = "none"
            binding_st = "unknown"

    return WarehouseDirectoryItem(
        canonical_name=name,
        physical_division_label=phys_label,
        operational_divisions=op_divs,
        location=loc,
        site_type=site_t,
        is_active=is_active,
        has_accounting_stock=has_rem_stock,
        has_free_stock=has_fs_stock,
        has_valid_free_stock=has_vfs_stock,
        binding_source=binding_src,
        binding_status=binding_st,
    )


async def fetch_warehouse_directory_data() -> List[WarehouseDirectoryItem]:
    """Виконує пакетне отримання даних з БД без N+1 запитів."""
    # 1. ValidWarehouseAdmin
    warehouses = (
        await ValidWarehouseAdmin.select(
            ValidWarehouseAdmin.name,
            ValidWarehouseAdmin.is_active,
        )
        .order_by(ValidWarehouseAdmin.name)
        .run()
    )

    # 2. FreeStock
    fs_raw = await FreeStock.raw(
        "SELECT warehouse, division, BOOL_OR(free_qty > 0) AS has_pos_free "
        "FROM free_stock WHERE warehouse IS NOT NULL GROUP BY warehouse, division"
    )
    fs_map: Dict[str, Dict[str, Any]] = {}
    for r in fs_raw:
        wh = r["warehouse"]
        if wh not in fs_map:
            fs_map[wh] = {"divisions": set(), "has_pos_free": False}
        if r.get("division"):
            fs_map[wh]["divisions"].add(r["division"])
        if r.get("has_pos_free"):
            fs_map[wh]["has_pos_free"] = True

    # 3. ValidFreeStock
    vfs_raw = await ValidFreeStock.raw(
        "SELECT warehouse, division, BOOL_OR(free_qty > 0) AS has_pos_free "
        "FROM valid_free_stock WHERE warehouse IS NOT NULL GROUP BY warehouse, division"
    )
    vfs_map: Dict[str, Dict[str, Any]] = {}
    for r in vfs_raw:
        wh = r["warehouse"]
        if wh not in vfs_map:
            vfs_map[wh] = {"divisions": set(), "has_pos_free": False}
        if r.get("division"):
            vfs_map[wh]["divisions"].add(r["division"])
        if r.get("has_pos_free"):
            vfs_map[wh]["has_pos_free"] = True

    # 4. Remains
    rem_raw = await Remains.raw(
        "SELECT warehouse, BOOL_OR(buh > 0) AS has_pos_buh "
        "FROM remains WHERE warehouse IS NOT NULL GROUP BY warehouse"
    )
    rem_map: Dict[str, bool] = {
        r["warehouse"]: bool(r.get("has_pos_buh"))
        for r in rem_raw
        if r.get("warehouse")
    }

    items: List[WarehouseDirectoryItem] = []
    for w in warehouses:
        w_name = w.get("name")
        if not w_name:
            continue
        w_active = bool(w.get("is_active", True))

        fs_entry = fs_map.get(w_name, {"divisions": set(), "has_pos_free": False})
        vfs_entry = vfs_map.get(w_name, {"divisions": set(), "has_pos_free": False})
        has_rem = rem_map.get(w_name, False)

        item = build_directory_item(
            name=w_name,
            is_active=w_active,
            fs_divs=fs_entry["divisions"],
            has_fs_stock=fs_entry["has_pos_free"],
            vfs_divs=vfs_entry["divisions"],
            has_vfs_stock=vfs_entry["has_pos_free"],
            has_rem_stock=has_rem,
        )
        items.append(item)

    return items


router = APIRouter(
    prefix="/data",
    tags=["Довідник складів"],
)


@router.get(
    "/warehouses/directory",
    response_model=List[WarehouseDirectoryItem],
    summary="Отримати довідник складів та операційних підрозділів",
    description=(
        "Повертає реєстр складів із встановленими операційними прив'язками, "
        "ознаками активності та наявності залишків. "
        "За замовчуванням повертає тільки активні склади. "
        "Включення неактивних складів (active_only=false) дозволено лише адміністраторам."
    ),
)
async def get_warehouse_directory(
    operational_division: Optional[str] = Query(
        None,
        description="Фільтр за операційним підрозділом (по встановлених зв'язках)",
    ),
    location: Optional[str] = Query(
        None,
        description="Фільтр за населеним пунктом (напр. 'Коротич', 'м.Балаклія')",
    ),
    active_only: bool = Query(
        True,
        description="Повертати лише активні склади (за замовчуванням true)",
    ),
    current_user: Users = Depends(get_current_telegram_user),
) -> List[WarehouseDirectoryItem]:
    # Перевірка прав для перегляду неактивних складів
    if not active_only and not getattr(current_user, "is_admin", False):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Перегляд неактивних складів дозволено лише адміністраторам.",
        )

    all_items = await fetch_warehouse_directory_data()

    # Фільтрація
    filtered_items: List[WarehouseDirectoryItem] = []
    for it in all_items:
        if active_only and not it.is_active:
            continue

        if operational_division:
            req_div = operational_division.strip().lower()
            if not any(req_div == d.strip().lower() for d in it.operational_divisions):
                continue

        if location:
            req_loc = location.strip().lower()
            if not it.location:
                continue
            it_loc = it.location.strip().lower()
            if req_loc not in it_loc and it_loc not in req_loc:
                continue

        filtered_items.append(it)

    return filtered_items
