import asyncio
from new_agri_bot_backend.tables import DeliveryNotifications

async def check_db():
    print("--- СОДЕРЖИМОЕ ТАБЛИЦЫ DeliveryNotifications ---")
    notes = await DeliveryNotifications.select().run()
    if not notes:
        print("Таблица пуста.")
    else:
        for n in notes:
            print(f"ID: {n['id']} | DeliveryID: {n['delivery_id']} | TelegramID: {n['telegram_id']} | MsgID: {n['message_id']} | Event: {n['event_type']}")
    print("-------------------------------------------------")

if __name__ == "__main__":
    asyncio.run(check_db())
