# app/data_loader.py
import asyncio
from concurrent.futures import ThreadPoolExecutor
import pandas as pd

# Импорты моделей Piccolo ORM
from .tables import (
    AvailableStock,
    Remains,
    Submissions,
    Payment,
    MovedData,
    ProductGuide,
)

# Импорты функций обработки данных
from .data_processing import (
    process_av_stock,
    process_remains_reg,
    process_submissions,
    process_payment,
    process_moved_data,
)

# Пул потоков для выполнения синхронных операций Pandas
executor = ThreadPoolExecutor(max_workers=4)


async def run_in_threadpool(func, *args):
    """Обертка для запуска синхронных функций в отдельном потоке."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(executor, func, *args)


async def save_processed_data_to_db(
    av_stock_content: bytes,
    remains_content: bytes,
    submissions_content: bytes,
    payment_content: bytes,
    moved_content: bytes,
):
    """
    Асинхронная функция для обработки и сохранения данных в базу данных.
    Оркестрирует вызовы синхронных функций обработки и асинхронных операций с БД.
    """
    print("Начало обработки и сохранения данных...")

    # 1. Запуск обработки Excel-файлов в отдельном потове
    df_av_stock = await run_in_threadpool(process_av_stock, av_stock_content)
    df_remains = await run_in_threadpool(process_remains_reg, remains_content)
    df_submissions = await run_in_threadpool(process_submissions, submissions_content)
    df_payment = await run_in_threadpool(process_payment, payment_content)
    df_moved = await run_in_threadpool(process_moved_data, moved_content)

    print("Данные Excel обработаны в DataFrame. Начинаем сохранение в БД...")

    # 2. Очищення існуючих даних у таблицях (ВАЖЛИВО: ПЕРЕКОНАЙТЕСЯ, ЩО ЦЕ ТЕ, ЩО ВАМ ПОТРІБНО)
    # ЦЕ ВИДАЛИТЬ УСІ ЗАПИСИ з відповідних таблиць перед новою вставкою.
    # Якщо вам потрібна інша логіка (наприклад, оновлення або upsert), реалізуйте її тут.
    await AvailableStock.delete().run()  # <--- ВИПРАВЛЕНО
    await Remains.delete().run()  # <--- ВИПРАВЛЕНО
    await Submissions.delete().run()  # <--- ВИПРАВЛЕНО
    await Payment.delete().run()  # <--- ВИПРАВЛЕНО
    await MovedData.delete().run()  # <--- ВИПРАВЛЕНО
    # ProductGuide, ймовірно, не слід очищати кожного разу,
    # оскільки це "довідник", який оновлюється рідше.
    # await ProductGuide.delete().run() # <--- ВИПРАВЛЕНО, якщо потрібно

    print("Старі дані з таблиць видалено. Починаємо вставку нових даних...")

    # 3. Вставка новых данных из DataFrame в соответствующие таблицы Piccolo
    # Вам нужно будет преобразовать DataFrame в список словарей, где ключи -
    # это имена колонок в базе данных, а значения - данные.
    # Убедитесь, что имена колонок DataFrame соответствуют полям в вашей модели Piccolo.

    if not df_av_stock.empty:
        records_av_stock = df_av_stock.to_dict(orient="records")
        await AvailableStock.objects().insert(*records_av_stock).run()
        print(f"Вставлено {len(records_av_stock)} записей в AvailableStock.")
    else:
        print("DataFrame для AvailableStock пуст, пропускаем вставку.")

    # TODO: Дополните аналогичную логику для Remains, Submissions, Payment, MovedData, ProductGuide
    # Пример:
    if not df_remains.empty:
        records_remains = df_remains.to_dict(orient="records")
        await Remains.objects().insert(*records_remains).run()
        print(f"Вставлено {len(records_remains)} записей в Remains.")
    else:
        print("DataFrame для Remains пуст, пропускаем вставку.")

    if not df_submissions.empty:
        records_submissions = df_submissions.to_dict(orient="records")
        await Submissions.objects().insert(*records_submissions).run()
        print(f"Вставлено {len(records_submissions)} записей в Submissions.")
    else:
        print("DataFrame для Submissions пуст, пропускаем вставку.")

    if not df_payment.empty:
        records_payment = df_payment.to_dict(orient="records")
        await Payment.objects().insert(*records_payment).run()
        print(f"Вставлено {len(records_payment)} записей в Payment.")
    else:
        print("DataFrame для Payment пуст, пропускаем вставку.")

    if not df_moved.empty:
        records_moved = df_moved.to_dict(orient="records")
        await MovedData.objects().insert(*records_moved).run()
        print(f"Вставлено {len(records_moved)} записей в MovedData.")
    else:
        print("DataFrame для MovedData пуст, пропускаем вставку.")

    # !!! ВНИМАНИЕ: Если ProductGuide - это справочник, который не всегда
    # должен очищаться и перезаписываться полностью, вам потребуется
    # более сложная логика upsert (обновить или вставить),
    # или отдельный эндпоинт для его загрузки.
    # Если же он также очищается и перезаписывается из отдельного файла,
    # то обработайте его здесь:
    # df_product_guide = await run_in_threadpool(process_product_guide, product_guide_content)
    # await ProductGuide.objects().delete().run()
    # if not df_product_guide.empty:
    #     records_product_guide = df_product_guide.to_dict(orient='records')
    #     await ProductGuide.objects().insert(*records_product_guide).run()
    #     print(f"Вставлено {len(records_product_guide)} записей в ProductGuide.")
    # else:
    #     print("DataFrame для ProductGuide пуст, пропускаем вставку.")

    print("Все данные успешно сохранены в базу данных.")
