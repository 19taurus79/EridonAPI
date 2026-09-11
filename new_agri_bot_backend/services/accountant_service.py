import asyncio
import email
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import html
import logging
import smtplib
from urllib.parse import quote

from aiogram.types import BufferedInputFile
from ..config import (
    bot,
    SMTP_HOST,
    SMTP_PORT,
    SMTP_USER,
    SMTP_PASSWORD,
    SMTP_FROM,
    SMTP_USE_TLS,
    SMTP_USE_SSL,
)

logger = logging.getLogger("agri_bot")


def _normalize_orders_data(delivery_data: dict, orders: list = None, items: list = None) -> list:
    """
    Нормалізує вхідні дані замовлень/товарів у структуру:
    [
        {
            "order_ref": "ТЕ-00062984",
            "client": "Назва клієнта",
            "manager": "Менеджер",
            "address": "Адреса",
            "items": [ ... ]
        }, ...
    ]
    Працює як з новою структурою (orders), так і з плоским списком товарів (items).
    """
    source_list = orders if orders is not None else (items or [])
    if not source_list:
        return []

    # Перевіряємо перший елемент: чи це вже згруповане замовлення (має ключ 'items')
    first = source_list[0]
    first_dict = first if isinstance(first, dict) else (first.dict() if hasattr(first, "dict") else {})

    if "items" in first_dict and isinstance(first_dict["items"], list):
        normalized = []
        for o in source_list:
            o_dict = o if isinstance(o, dict) else (o.dict() if hasattr(o, "dict") else {})
            items_list = []
            for it in o_dict.get("items", []):
                it_dict = it if isinstance(it, dict) else (it.dict() if hasattr(it, "dict") else {})
                items_list.append(it_dict)
            normalized.append({
                "order_ref": str(o_dict.get("order_ref") or "—").strip(),
                "client": str(o_dict.get("client") or delivery_data.get("client") or "Не вказано").strip(),
                "manager": str(o_dict.get("manager") or delivery_data.get("manager") or "").strip(),
                "address": str(o_dict.get("address") or delivery_data.get("address") or "").strip(),
                "items": items_list
            })
        return normalized

    # Якщо передано плоский список позицій (fallback)
    grouped = {}
    for it in source_list:
        it_dict = it if isinstance(it, dict) else (it.dict() if hasattr(it, "dict") else {})
        o_ref = str(it_dict.get("order_ref") or it_dict.get("orderRef") or it_dict.get("order") or "—").strip()
        client = str(it_dict.get("client") or delivery_data.get("client") or "Не вказано").strip()
        manager = str(it_dict.get("manager") or delivery_data.get("manager") or "").strip()
        address = str(it_dict.get("address") or delivery_data.get("address") or "").strip()
        key = (client, o_ref)
        if key not in grouped:
            grouped[key] = {
                "order_ref": o_ref,
                "client": client,
                "manager": manager,
                "address": address,
                "items": []
            }
        grouped[key]["items"].append(it_dict)
    return list(grouped.values())


def format_delivery_subject(delivery_data: dict, orders: list = None) -> str:
    """Формує тему листа для бухгалтера з урахуванням клієнтів та доповнень"""
    orders_data = _normalize_orders_data(delivery_data, orders)
    
    unique_clients = []
    unique_order_refs = []
    managers = []
    
    for o in orders_data:
        c = o.get("client")
        if c and c != "Не вказано" and c not in unique_clients:
            unique_clients.append(c)
        ref = o.get("order_ref")
        if ref and ref != "—" and ref not in unique_order_refs:
            unique_order_refs.append(ref)
        mgr = o.get("manager")
        if mgr and mgr not in managers:
            managers.append(mgr)

    if not unique_clients and delivery_data.get("client"):
        unique_clients = [delivery_data.get("client")]

    ttn = str(delivery_data.get("ttn") or "").strip()
    is_np = bool(ttn and ttn != "Не вказано")
    date = str(delivery_data.get("delivery_date") or delivery_data.get("date") or "")

    client_part = ", ".join(unique_clients[:2])
    if len(unique_clients) > 2:
        client_part += f" (+{len(unique_clients) - 2})"

    order_part = ""
    if unique_order_refs:
        order_part = f" [Доп: {', '.join(unique_order_refs[:2])}{'...' if len(unique_order_refs) > 2 else ''}]"

    mgr_str = ", ".join(managers) if managers else str(delivery_data.get("manager") or "")
    mgr_part = f" | Менеджер: {mgr_str}" if mgr_str else ""

    if is_np:
        return f"[Нова Пошта] Відомість: {client_part}{order_part} | ТТН {ttn}{mgr_part}"
    return f"[Доставка] Відомість: {client_part}{order_part} | Дата: {date}{mgr_part}"


def generate_printable_html(delivery_data: dict, orders: list = None, custom_comment: str = None, items: list = None) -> str:
    """
    Генерує чистий HTML-документ печаткової форми відомості відвантаження
    зі структурованою групуванням по Доповненнях та Клієнтах.
    """
    orders_data = _normalize_orders_data(delivery_data, orders, items)

    raw_ttn = str(delivery_data.get("ttn") or "").strip()
    has_ttn = bool(raw_ttn and raw_ttn != "Не вказано")
    ttn = html.escape(raw_ttn if has_ttn else "Не вказано")
    title_suffix = f" (ТТН {ttn})" if has_ttn else ""
    date = html.escape(str(delivery_data.get("delivery_date") or delivery_data.get("date") or ""))
    comment = html.escape(str(delivery_data.get("comment") or ""))
    additional_note = html.escape(str(custom_comment or ""))

    # Збираємо унікальних клієнтів та менеджерів для загальної шапки
    unique_clients = []
    unique_managers = []
    for o in orders_data:
        c = o.get("client")
        if c and c != "Не вказано" and c not in unique_clients:
            unique_clients.append(c)
        m = o.get("manager")
        if m and m not in unique_managers:
            unique_managers.append(m)

    if not unique_clients and delivery_data.get("client"):
        unique_clients = [delivery_data.get("client")]
    if not unique_managers and delivery_data.get("manager"):
        unique_managers = [delivery_data.get("manager")]

    clients_header_str = ", ".join([html.escape(c) for c in unique_clients]) if unique_clients else "Не вказано"
    managers_header_str = ", ".join([html.escape(m) for m in unique_managers]) if unique_managers else "Не вказано"

    orders_count = len(orders_data)
    total_qty = 0
    total_items_count = 0
    sections_html = []

    for order_idx, order in enumerate(orders_data, 1):
        order_ref = html.escape(str(order.get("order_ref") or "—"))
        order_client = html.escape(str(order.get("client") or "Не вказано"))
        order_manager = html.escape(str(order.get("manager") or ""))
        order_address = html.escape(str(order.get("address") or ""))

        order_items = order.get("items") or []
        order_qty = 0
        order_rows = []

        for item_idx, it in enumerate(order_items, 1):
            total_items_count += 1
            prod = html.escape(str(it.get("nomenclature") or it.get("product") or ""))
            qty = float(it.get("quantity") or 0)
            order_qty += qty
            total_qty += qty

            parties = it.get("parties") or []
            parties_str_list = []
            if isinstance(parties, list):
                for p in parties:
                    if isinstance(p, dict):
                        p_name = p.get("party") or ""
                        p_q = p.get("party_quantity")
                        if p_q is None or p_q == "":
                            p_q = p.get("moved_q") or 0
                        parties_str_list.append(f"{html.escape(str(p_name))} ({p_q})")
                    elif isinstance(p, str):
                        parties_str_list.append(html.escape(p))
            parties_cell = ", ".join(parties_str_list) if parties_str_list else "—"

            order_rows.append(f"""
                <tr>
                    <td style="text-align: center; color: #64748b;">{item_idx}</td>
                    <td style="font-weight: 500;">{prod}</td>
                    <td style="text-align: center; font-weight: bold;">{qty:g}</td>
                    <td>{parties_cell}</td>
                </tr>
            """)

        items_tbody = "\n".join(order_rows) if order_rows else """<tr><td colspan="4" style="text-align: center; color: #94a3b8; padding: 8px;">(позиції відсутні)</td></tr>"""

        # Заголовок секції даного замовлення/доповнення
        header_meta = [
            f"<span>🏢 <strong>Клієнт:</strong> {order_client}</span>",
            f"<span>📄 <strong>Доповнення:</strong> <span class=\"order-badge\">#{order_ref}</span></span>",
        ]
        if order_manager:
            header_meta.append(f"<span>👨‍💼 <strong>Менеджер:</strong> {order_manager}</span>")
        if order_address:
            header_meta.append(f"<span>📍 <strong>Адреса:</strong> {order_address}</span>")

        meta_html = " &nbsp;│&nbsp; ".join(header_meta)

        subtotal_html = ""
        if len(order_items) > 1:
            subtotal_html = f"""
                <tr style="background: #f8fafc; font-weight: 600; font-size: 12px;">
                    <td colspan="2" style="text-align: right; color: #475569;">Разом по доповненню #{order_ref}:</td>
                    <td style="text-align: center; color: #0284c7;">{order_qty:g}</td>
                    <td></td>
                </tr>
            """

        sections_html.append(f"""
            <!-- Секція замовлення: {order_ref} -->
            <tr style="background: #f1f5f9; border-top: 2px solid #94a3b8;">
                <td colspan="4" style="padding: 9px 12px;">
                    <div style="font-size: 13px; color: #1e293b; line-height: 1.6;">
                        {meta_html}
                    </div>
                </td>
            </tr>
            {items_tbody}
            {subtotal_html}
        """)

    table_body = "\n".join(sections_html)

    html_content = f"""<!DOCTYPE html>
<html lang="uk">
<head>
    <meta charset="UTF-8">
    <title>Відомість доставки Нова Пошта - ТТН {ttn}</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 20px;
            color: #111;
            background: #fff;
            font-size: 13px;
            line-height: 1.4;
        }}
        .header {{
            text-align: center;
            border-bottom: 2px solid #0284c7;
            padding-bottom: 12px;
            margin-bottom: 20px;
        }}
        h2 {{
            margin: 0 0 6px 0;
            color: #0f172a;
            font-size: 20px;
        }}
        .meta {{
            font-size: 13px;
            color: #64748b;
        }}
        .info-grid {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 10px;
            background: #f8fafc;
            padding: 14px;
            border-radius: 8px;
            border: 1px solid #e2e8f0;
            margin-bottom: 20px;
            font-size: 13px;
        }}
        .info-row strong {{
            color: #475569;
        }}
        .ttn-badge {{
            display: inline-block;
            background: #e0f2fe;
            color: #0369a1;
            font-weight: bold;
            padding: 2px 8px;
            border-radius: 4px;
            letter-spacing: 0.5px;
        }}
        .order-badge {{
            display: inline-block;
            background: #e2e8f0;
            color: #0f172a;
            font-weight: bold;
            padding: 1px 6px;
            border-radius: 4px;
            font-family: monospace;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin-bottom: 30px;
            font-size: 13px;
        }}
        th, td {{
            border: 1px solid #cbd5e1;
            padding: 8px 10px;
            text-align: left;
        }}
        th {{
            background-color: #0f172a;
            color: #ffffff;
            font-weight: 600;
        }}
        .signatures {{
            margin-top: 40px;
            display: flex;
            justify-content: space-between;
            padding: 0 20px;
        }}
        .sign-line {{
            width: 240px;
            border-top: 1px solid #000;
            text-align: center;
            padding-top: 5px;
            font-size: 12px;
        }}
        @media print {{
            body {{ margin: 0; font-size: 12px; }}
            .info-grid {{ border: 1px solid #000; }}
            th {{ background-color: #333 !important; color: #fff !important; -webkit-print-color-adjust: exact; }}
        }}
    </style>
</head>
<body>
    <div class="header">
        <h2>Відомість на відвантаження{title_suffix}</h2>
        <div class="meta">Згенеровано з автоматизованої системи логістики | Дата: {date}</div>
    </div>

    <div class="info-grid">
        <div class="info-row"><strong>{"Клієнт" if len(unique_clients) <= 1 else "Клієнти"}:</strong> {clients_header_str}</div>
        {f'<div class="info-row"><strong>Номер ТТН:</strong> <span class="ttn-badge">{ttn}</span></div>' if has_ttn else '<div class="info-row"><strong>Тип:</strong> Доставка / Відвантаження</div>'}
        <div class="info-row"><strong>Менеджер:</strong> {managers_header_str}</div>
        <div class="info-row"><strong>Дата відвантаження:</strong> {date}</div>
        <div class="info-row"><strong>Кількість заявок / доповнень:</strong> {orders_count}</div>
        <div class="info-row"><strong>Всього найменувань товару:</strong> {total_items_count}</div>
        {f'<div class="info-row" style="grid-column: span 2;"><strong>Примітка доставки:</strong> {comment}</div>' if comment else ''}
        {f'<div class="info-row" style="grid-column: span 2; color: #0056b3;"><strong>Коментар для бухгалтера:</strong> {additional_note}</div>' if additional_note else ''}
    </div>

    <table>
        <thead>
            <tr>
                <th style="width: 5%; text-align: center;">№</th>
                <th style="width: 48%;">Товар / Номенклатура</th>
                <th style="width: 14%; text-align: center;">К-сть</th>
                <th style="width: 33%;">Складські партії</th>
            </tr>
        </thead>
        <tbody>
            {table_body}
        </tbody>
        <tfoot>
            <tr style="background: #fafafa; font-weight: bold; font-size: 14px;">
                <td colspan="2" style="text-align: right;">Всього по відомості:</td>
                <td style="text-align: center; color: #0284c7;">{total_qty:g}</td>
                <td></td>
            </tr>
        </tfoot>
    </table>

    <div class="signatures">
        <div>
            <div>Відпустив: _____________________</div>
            <div class="sign-line">(підпис / П.І.Б.)</div>
        </div>
        <div>
            <div>Прийняв (Бухгалтерія): _____________________</div>
            <div class="sign-line">(підпис / П.І.Б.)</div>
        </div>
    </div>
</body>
</html>
"""
    return html_content


def generate_mailto_url(accountant_email: str, delivery_data: dict, orders: list = None, custom_comment: str = None, items: list = None) -> str:
    """
    Формує посилання mailto для відкриття поштової програми (Outlook, Thunderbird тощо)
    зі структурованим групуванням по доповненнях і клієнтах.
    """
    orders_data = _normalize_orders_data(delivery_data, orders, items)
    subject = format_delivery_subject(delivery_data, orders_data)

    ttn = (delivery_data.get("ttn") or "").strip()
    date = delivery_data.get("delivery_date") or delivery_data.get("date") or ""
    is_np = bool(ttn and ttn != "Не вказано")
    delivery_type_str = "Нова Пошта" if is_np else "Доставка / Самовивіз"
    ttn_line = f"- ТТН Нова Пошта: {ttn}\n" if is_np else ""

    order_blocks = []
    total_qty = 0

    for idx, order in enumerate(orders_data, 1):
        o_ref = order.get("order_ref") or "—"
        o_client = order.get("client") or "Не вказано"
        o_manager = order.get("manager") or ""
        o_address = order.get("address") or ""

        items_lines = []
        for it in order.get("items", []):
            prod = it.get("nomenclature") or it.get("product") or ""
            qty = float(it.get("quantity") or 0)
            total_qty += qty

            parties = it.get("parties") or []
            p_strs = []
            for p in parties:
                if isinstance(p, dict):
                    p_name = p.get("party") or ""
                    p_q = p.get("party_quantity")
                    if p_q is None or p_q == "":
                        p_q = p.get("moved_q") or 0
                    p_strs.append(f"{p_name}: {p_q}")
                elif isinstance(p, str):
                    p_strs.append(p)
            parties_part = f" [Партії: {', '.join(p_strs)}]" if p_strs else ""
            items_lines.append(f"  • {prod} — {qty:g} шт{parties_part}")

        items_text = "\n".join(items_lines) if items_lines else "  (товари не вказані)"

        mgr_line = f"- Менеджер: {o_manager}\n" if o_manager else ""
        addr_line = f"- Адреса: {o_address}\n" if o_address else ""

        block = f"""━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🏢 Клієнт: {o_client}
📄 Доповнення: #{o_ref}
{mgr_line}{addr_line}Товари та складські партії:
{items_text}"""
        order_blocks.append(block)

    all_orders_text = "\n\n".join(order_blocks) if order_blocks else "(замовлення відсутні)"

    body = f"""Доброго дня!

Інформація щодо відвантаження ({delivery_type_str}):
{ttn_line}- Дата: {date}
- Кількість заявок / доповнень: {len(orders_data)}

{all_orders_text}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Всього товарів: {total_qty:g} шт
"""
    if custom_comment:
        body += f"\nКоментар для бухгалтера: {custom_comment}\n"
    if delivery_data.get("comment"):
        body += f"Примітка доставки: {delivery_data.get('comment')}\n"

    body += "\n---\nЗгенеровано з додатку логістики"

    return f"mailto:{accountant_email}?subject={quote(subject)}&body={quote(body)}"


async def send_accountant_telegram(
    telegram_id: int,
    delivery_data: dict,
    orders: list = None,
    printable_html: str = "",
    custom_comment: str = None,
    items: list = None
) -> dict:
    """
    Надсилає структуроване повідомлення та файл друкованої форми в Telegram бухгалтеру
    з чітким розділенням на клієнтів та доповнення.
    """
    if not telegram_id or int(telegram_id) == 0:
        return {"success": False, "error": "Telegram ID бухгалтера не вказано"}

    orders_data = _normalize_orders_data(delivery_data, orders, items)

    ttn = (delivery_data.get("ttn") or "").strip()
    date = html.escape(str(delivery_data.get("delivery_date") or delivery_data.get("date") or ""))
    is_np = bool(ttn and ttn != "Не вказано")

    if is_np:
        header_title = "📦 <b>[Нова Пошта] ВІДОМІСТЬ НА ВІДВАНТАЖЕННЯ</b>"
        ttn_info = f"📦 <b>ТТН:</b> <code>{html.escape(ttn)}</code>\n"
        tracking_link = f"\n🔗 <a href=\"https://novaposhta.ua/tracking/{html.escape(ttn)}\">Відстежити на сайті Нової Пошти</a>"
    else:
        header_title = "🚚 <b>[Доставка] ВІДОМІСТЬ НА ВІДВАНТАЖЕННЯ</b>"
        ttn_info = ""
        tracking_link = ""

    blocks = []
    total_qty = 0

    for order in orders_data:
        o_ref = html.escape(str(order.get("order_ref") or "—"))
        o_client = html.escape(str(order.get("client") or "Не вказано"))
        o_manager = html.escape(str(order.get("manager") or ""))
        o_address = html.escape(str(order.get("address") or ""))

        items_lines = []
        for it in order.get("items", []):
            prod = html.escape(str(it.get("nomenclature") or it.get("product") or ""))
            qty = float(it.get("quantity") or 0)
            total_qty += qty

            parties = it.get("parties") or []
            p_strs = []
            for p in parties:
                if isinstance(p, dict):
                    p_name = html.escape(str(p.get("party") or ""))
                    p_q = p.get("party_quantity")
                    if p_q is None or p_q == "":
                        p_q = p.get("moved_q") or 0
                    p_strs.append(f"{p_name} ({p_q})")
                elif isinstance(p, str):
                    p_strs.append(html.escape(p))
            parties_part = f"\n   ↳ <i>Партії: {', '.join(p_strs)}</i>" if p_strs else ""
            items_lines.append(f"▫️ <b>{prod}</b> — <b>{qty:g}</b> шт{parties_part}")

        items_text = "\n".join(items_lines) if items_lines else "<i>(товари не вказані)</i>"

        mgr_line = f"👨‍💼 <b>Менеджер:</b> {o_manager}\n" if o_manager else ""
        addr_line = f"📍 <b>Адреса:</b> {o_address}\n" if o_address else ""

        block = (
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🏢 <b>Клієнт:</b> {o_client}\n"
            f"📄 <b>Доповнення:</b> <code>#{o_ref}</code>\n"
            f"{mgr_line}{addr_line}"
            f"<b>Товари та партії:</b>\n{items_text}"
        )
        blocks.append(block)

    orders_content = "\n\n".join(blocks)

    caption_text = (
        f"{header_title}\n\n"
        f"{ttn_info}"
        f"📅 <b>Дата:</b> {date}\n"
        f"📋 <b>Кількість заявок:</b> {len(orders_data)}\n\n"
        f"{orders_content}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>Всього товару:</b> <b>{total_qty:g}</b> шт\n"
    )

    if custom_comment:
        caption_text += f"\n💬 <b>Коментар:</b> {html.escape(custom_comment)}\n"

    if tracking_link:
        caption_text += tracking_link

    # Захист від перевищення ліміту Telegram (4096 символів)
    if len(caption_text) > 4000:
        logger.warning(f"Telegram message too long ({len(caption_text)} chars). Compacting.")
        compact_blocks = []
        for o in orders_data:
            c_ref = html.escape(str(o.get("order_ref") or "—"))
            c_client = html.escape(str(o.get("client") or "Не вказано"))
            c_cnt = len(o.get("items", []))
            compact_blocks.append(f"• <b>{c_client}</b> (Доп: <code>#{c_ref}</code>) — {c_cnt} поз.")

        caption_text = (
            f"{header_title}\n\n"
            f"{ttn_info}"
            f"📅 <b>Дата:</b> {date}\n"
            f"📋 <b>Заявки у відомості ({len(orders_data)}):</b>\n"
            + "\n".join(compact_blocks)
            + f"\n\n📊 <b>Всього товару:</b> <b>{total_qty:g}</b> шт\n"
            + f"\n📄 <i>Повний перелік позицій і складських партій дивіться у прикріпленому файлі відомості.</i>\n"
        )
        if custom_comment:
            caption_text += f"\n💬 <b>Коментар:</b> {html.escape(custom_comment)}\n"
        if tracking_link:
            caption_text += tracking_link

    try:
        # 1. Текстове повідомлення
        await bot.send_message(
            chat_id=telegram_id,
            text=caption_text,
            parse_mode="HTML",
            disable_web_page_preview=True
        )

        # 2. Вкладення HTML-файлу печаткової форми
        if printable_html:
            html_bytes = printable_html.encode("utf-8")
            file_tag = ttn if is_np else str(delivery_data.get('id') or 'delivery')
            doc_file = BufferedInputFile(
                file=html_bytes,
                filename=f"Vidomist_{file_tag}.html"
            )
            first_client = orders_data[0].get("client") if orders_data else (delivery_data.get("client") or "")
            doc_caption = f"📄 Відомість ({'ТТН: ' + ttn if is_np else first_client})"
            await bot.send_document(
                chat_id=telegram_id,
                document=doc_file,
                caption=doc_caption
            )
        return {"success": True}
    except Exception as e:
        logger.error(f"Error sending delivery to accountant in Telegram ({telegram_id}): {e}")
        return {"success": False, "error": str(e)}


def _send_email_sync(to_email: str, subject: str, html_body: str, attachment_content: str, filename: str):
    msg = MIMEMultipart("mixed")
    msg["From"] = SMTP_FROM or SMTP_USER
    msg["To"] = to_email
    msg["Subject"] = subject

    # Альтернативна частина з HTML
    msg_alt = MIMEMultipart("alternative")
    part_html = MIMEText(html_body, "html", "utf-8")
    msg_alt.attach(part_html)
    msg.attach(msg_alt)

    # Вкладення печаткової форми
    att = MIMEApplication(attachment_content.encode("utf-8"), _subtype="html")
    att.add_header("Content-Disposition", "attachment", filename=filename)
    msg.attach(att)

    # Надсилання через SMTP
    if SMTP_USE_SSL:
        server = smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=15)
    else:
        server = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=15)
        if SMTP_USE_TLS:
            server.starttls()

    try:
        if SMTP_USER and SMTP_PASSWORD:
            server.login(SMTP_USER, SMTP_PASSWORD)
        server.send_message(msg)
    finally:
        server.quit()


async def send_accountant_email(
    to_email: str,
    delivery_data: dict,
    orders: list = None,
    printable_html: str = "",
    custom_comment: str = None,
    items: list = None
) -> dict:
    """
    Надсилає лист на пошту бухгалтера з таблицею та вкладенням HTML печаткової форми
    """
    if not to_email:
        return {"success": False, "error": "Email бухгалтера не вказано"}

    if not SMTP_HOST:
        logger.info("SMTP_HOST не налаштовано в .env. Пропускаємо автоматичну відправку Email.")
        return {
            "success": False, 
            "error": "SMTP сервер не налаштовано в .env (SMTP_HOST)",
            "skipped": True
        }

    ttn = (delivery_data.get("ttn") or "").strip()
    is_np = bool(ttn and ttn != "Не вказано")
    subject = format_delivery_subject(delivery_data, orders or items)
    file_tag = ttn if is_np else str(delivery_data.get('id') or 'delivery')
    filename = f"Vidomist_{file_tag}.html"

    try:
        await asyncio.to_thread(
            _send_email_sync,
            to_email=to_email,
            subject=subject,
            html_body=printable_html,
            attachment_content=printable_html,
            filename=filename
        )
        return {"success": True}
    except Exception as e:
        logger.error(f"Error sending email to {to_email}: {e}")
        return {"success": False, "error": str(e)}
