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


def generate_printable_html(delivery_data: dict, items: list, custom_comment: str = None) -> str:
    """
    Генерує чистий HTML-документ печаткової форми відомості відвантаження
    """
    client = html.escape(str(delivery_data.get("client") or "Не вказано"))
    manager = html.escape(str(delivery_data.get("manager") or "Не вказано"))
    raw_ttn = str(delivery_data.get("ttn") or "").strip()
    has_ttn = bool(raw_ttn and raw_ttn != "Не вказано")
    ttn = html.escape(raw_ttn if has_ttn else "Не вказано")
    title_suffix = f" (ТТН {ttn})" if has_ttn else ""
    address = html.escape(str(delivery_data.get("address") or "Не вказано"))
    contact = html.escape(str(delivery_data.get("contact") or ""))
    phone = html.escape(str(delivery_data.get("phone") or ""))
    date = html.escape(str(delivery_data.get("delivery_date") or delivery_data.get("date") or ""))
    comment = html.escape(str(delivery_data.get("comment") or ""))
    additional_note = html.escape(str(custom_comment or ""))

    rows_html = []
    total_qty = 0
    for idx, item in enumerate(items, 1):
        prod = html.escape(str(item.get("nomenclature") or item.get("product") or ""))
        order_ref = html.escape(str(item.get("order_ref") or item.get("orderRef") or item.get("order") or "—"))
        qty = float(item.get("quantity") or 0)
        total_qty += qty

        parties = item.get("parties") or []
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

        rows_html.append(f"""
            <tr>
                <td style="text-align: center;">{idx}</td>
                <td>{order_ref}</td>
                <td style="font-weight: 500;">{prod}</td>
                <td style="text-align: center; font-weight: bold;">{qty:g}</td>
                <td>{parties_cell}</td>
            </tr>
        """)

    table_rows = "\n".join(rows_html)

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
            font-size: 14px;
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
            background-color: #f1f5f9;
            color: #334155;
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
            th {{ background-color: #eee !important; -webkit-print-color-adjust: exact; }}
        }}
    </style>
</head>
<body>
    <div class="header">
        <h2>Відомість на відвантаження{title_suffix}</h2>
        <div class="meta">Згенеровано з автоматизованої системи логістики | Дата: {date}</div>
    </div>

    <div class="info-grid">
        <div class="info-row"><strong>Клієнт:</strong> {client}</div>
        {f'<div class="info-row"><strong>Номер ТТН:</strong> <span class="ttn-badge">{ttn}</span></div>' if has_ttn else '<div class="info-row"><strong>Тип:</strong> Доставка / Відвантаження</div>'}
        <div class="info-row"><strong>Менеджер:</strong> {manager}</div>
        <div class="info-row"><strong>Дата відвантаження:</strong> {date}</div>
        <div class="info-row"><strong>Адреса / Відділення:</strong> {address}</div>
        <div class="info-row"><strong>Контакт / Телефон:</strong> {contact} {phone}</div>
        {f'<div class="info-row" style="grid-column: span 2;"><strong>Примітка доставки:</strong> {comment}</div>' if comment else ''}
        {f'<div class="info-row" style="grid-column: span 2; color: #0056b3;"><strong>Коментар для бухгалтера:</strong> {additional_note}</div>' if additional_note else ''}
    </div>

    <table>
        <thead>
            <tr>
                <th style="width: 4%; text-align: center;">№</th>
                <th style="width: 16%;">Доповнення</th>
                <th style="width: 40%;">Товар / Номенклатура</th>
                <th style="width: 12%; text-align: center;">К-сть</th>
                <th style="width: 28%;">Складські партії</th>
            </tr>
        </thead>
        <tbody>
            {table_rows}
        </tbody>
        <tfoot>
            <tr style="background: #fafafa; font-weight: bold;">
                <td colspan="3" style="text-align: right;">Всього:</td>
                <td style="text-align: center;">{total_qty:g}</td>
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


def format_delivery_subject(delivery_data: dict) -> str:
    """Формує тему листа для бухгалтера"""
    client = str(delivery_data.get("client") or "")
    ttn = str(delivery_data.get("ttn") or "").strip()
    manager = str(delivery_data.get("manager") or "")
    date = str(delivery_data.get("delivery_date") or delivery_data.get("date") or "")
    mgr_part = f" | Менеджер: {manager}" if manager else ""
    is_np = bool(ttn and ttn != "Не вказано")
    if is_np:
        return f"[Нова Пошта] Відомість: {client} | ТТН {ttn}{mgr_part}"
    return f"[Доставка] Відомість: {client} | Дата: {date}{mgr_part}"


def generate_mailto_url(accountant_email: str, delivery_data: dict, items: list, custom_comment: str = None) -> str:
    """
    Формує посилання mailto для відкриття поштової програми за замовчуванням (Outlook, Thunderbird тощо)
    """
    client = delivery_data.get("client") or ""
    ttn = (delivery_data.get("ttn") or "").strip()
    manager = delivery_data.get("manager") or ""
    date = delivery_data.get("delivery_date") or delivery_data.get("date") or ""
    
    subject = format_delivery_subject(delivery_data)
    
    items_lines = []
    for it in items:
        prod = it.get("nomenclature") or it.get("product") or ""
        order_ref = str(it.get("order_ref") or it.get("orderRef") or it.get("order") or "").strip()
        order_part = f" [Доповнення: {order_ref}]" if order_ref and order_ref != "—" else ""
        qty = it.get("quantity") or 0
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
        items_lines.append(f"• {prod}{order_part} — {qty} шт{parties_part}")

    items_text = "\n".join(items_lines) if items_lines else "(товари не вказані)"
    
    is_np = bool(ttn and ttn != "Не вказано")
    delivery_type_str = "Нова Пошта" if is_np else "Доставка / Самовивіз"
    ttn_line = f"- ТТН Нова Пошта: {ttn}\n" if is_np else ""

    body = f"""Доброго дня!

Інформація щодо відвантаження ({delivery_type_str}):
- Клієнт: {client}
- Менеджер: {manager}
{ttn_line}- Дата: {date}
- Адреса: {delivery_data.get("address") or "—"}

Товари та складські партії:
{items_text}
"""
    if custom_comment:
        body += f"\nКоментар: {custom_comment}\n"
    if delivery_data.get("comment"):
        body += f"Примітка доставки: {delivery_data.get('comment')}\n"

    body += "\n---\nЗгенеровано з додатку логістики"

    return f"mailto:{accountant_email}?subject={quote(subject)}&body={quote(body)}"


async def send_accountant_telegram(
    telegram_id: int,
    delivery_data: dict,
    items: list,
    printable_html: str,
    custom_comment: str = None
) -> dict:
    """
    Надсилає структуроване повідомлення та файл друкованої форми в Telegram бухгалтеру
    """
    if not telegram_id or int(telegram_id) == 0:
        return {"success": False, "error": "Telegram ID бухгалтера не вказано"}

    client = html.escape(str(delivery_data.get("client") or ""))
    manager = html.escape(str(delivery_data.get("manager") or ""))
    ttn = (delivery_data.get("ttn") or "").strip()
    date = html.escape(str(delivery_data.get("delivery_date") or delivery_data.get("date") or ""))
    address = html.escape(str(delivery_data.get("address") or ""))

    items_list = []
    for item in items:
        prod = html.escape(str(item.get("nomenclature") or item.get("product") or ""))
        order_ref = str(item.get("order_ref") or item.get("orderRef") or item.get("order") or "").strip()
        order_ref_part = f"\n   📄 <i>Доповнення: <code>{html.escape(order_ref)}</code></i>" if order_ref and order_ref != "—" else ""
        qty = item.get("quantity") or 0
        parties = item.get("parties") or []
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
        items_list.append(f"▫️ <b>{prod}</b> — <b>{qty}</b> шт{order_ref_part}{parties_part}")

    items_text = "\n".join(items_list) if items_list else "<i>(товари не вказані)</i>"

    is_np = bool(ttn and ttn != "Не вказано")
    if is_np:
        header_title = f"📦 <b>[Нова Пошта] ВІДОМІСТЬ НА ВІДВАНТАЖЕННЯ</b>"
        ttn_info = f"📦 <b>ТТН:</b> <code>{html.escape(ttn)}</code>\n"
        tracking_link = f"\n🔗 <a href=\"https://novaposhta.ua/tracking/{html.escape(ttn)}\">Відстежити на сайті Нової Пошти</a>"
    else:
        header_title = f"🚚 <b>[Доставка] ВІДОМІСТЬ НА ВІДВАНТАЖЕННЯ</b>"
        ttn_info = ""
        tracking_link = ""

    caption_text = (
        f"{header_title}\n\n"
        f"👤 <b>Клієнт:</b> {client}\n"
        f"👨‍💼 <b>Менеджер:</b> {manager}\n"
        f"{ttn_info}"
        f"📅 <b>Дата:</b> {date}\n"
        f"📍 <b>Адреса:</b> {address}\n\n"
        f"📋 <b>Товари та партії:</b>\n{items_text}\n"
    )

    if custom_comment:
        caption_text += f"\n💬 <b>Коментар:</b> {html.escape(custom_comment)}\n"

    if tracking_link:
        caption_text += tracking_link

    try:
        # 1. Надсилаємо текстове повідомлення
        await bot.send_message(
            chat_id=telegram_id,
            text=caption_text,
            parse_mode="HTML",
            disable_web_page_preview=True
        )

        # 2. Надсилаємо вкладений файл друкованої форми HTML
        html_bytes = printable_html.encode("utf-8")
        file_tag = ttn if is_np else str(delivery_data.get('id') or 'delivery')
        doc_file = BufferedInputFile(
            file=html_bytes,
            filename=f"Vidomist_{file_tag}.html"
        )
        await bot.send_document(
            chat_id=telegram_id,
            document=doc_file,
            caption=f"📄 Друкована форма відомості ({'ТТН: ' + ttn if is_np else client})"
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
    items: list,
    printable_html: str,
    custom_comment: str = None
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
    subject = format_delivery_subject(delivery_data)
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
