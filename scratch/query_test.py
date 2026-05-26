from new_agri_bot_backend.utils import extract_order_ref

note_text = """Контрагент: Авенга СФГ Харків
Менеджер: Шевцов Микола Петрович
Адреса: Самовивіз
Контакт:
Телефон:
Дата доставки: 2026-05-26
Коментар : САМОВИВІЗ. тест
📦 Доповнення: ТЕ-00007059
• Євро-лайтнінг Плюс, в.р. (10 л) — 120.0"""

print("Test extract_order_ref on notes:")
print(f"Result: {repr(extract_order_ref(note_text))}")
