# app/data_loader.py
import asyncio
import uuid
from concurrent.futures import ThreadPoolExecutor
import pandas as pd

from piccolo_conf import DB, DB_2

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

    # print("Старі дані з таблиць видалено. Починаємо вставку нових даних...")
    # 2.1 Создание справочника товаров
    av_stock_tmp = df_av_stock[["product", "line_of_business", "active_substance"]]
    remains_tmp = df_remains[["product", "line_of_business", "active_substance"]]
    submissions_tmp = df_submissions[
        ["product", "line_of_business", "active_ingredient"]
    ].rename(columns={"active_ingredient": "active_substance"})
    pr = pd.concat([av_stock_tmp, submissions_tmp, remains_tmp], ignore_index=True)
    product_guide = pr.drop_duplicates(["product"]).reset_index(drop=True)
    product_guide.insert(0, "id", product_guide.apply(lambda _: uuid.uuid4(), axis=1))

    # 3. Вставка новых данных из DataFrame в соответствующие таблицы Piccolo
    # Вам нужно будет преобразовать DataFrame в список словарей, где ключи -
    # это имена колонок в базе данных, а значения - данные.
    # Убедитесь, что имена колонок DataFrame соответствуют полям в вашей модели Piccolo.
    BATCH_SIZE = 1000
    if not product_guide.empty:
        await ProductGuide.delete(force=True).run()
        await ProductGuide.delete(force=True).run(node="DB_2")
        records_product_guide = product_guide.to_dict(orient="records")
        product_guide_raw = [ProductGuide(**item) for item in records_product_guide]
        for i in range(0, len(product_guide_raw), BATCH_SIZE):
            batch = product_guide_raw[i : i + BATCH_SIZE]
            rows = list(batch)
            await ProductGuide.insert().add(*rows).run()
            await ProductGuide.insert().add(*rows).run(node="DB_2")

    if not df_av_stock.empty:
        df_av_stock = df_av_stock.drop("active_substance", axis=1)
        await AvailableStock.delete(force=True).run()
        await AvailableStock.delete(force=True).run(node="DB_2")
        av_stock_data = df_av_stock.merge(
            product_guide, on="product", how="left", suffixes=("_av", "_guide")
        )
        av_stock_data = av_stock_data.drop(
            ["product", "line_of_business_guide", "active_substance"], axis=1
        )
        av_stock_data = av_stock_data.rename(columns={"id": "product"})
        av_stock_data = av_stock_data.rename(
            columns={"line_of_business_av": "line_of_business"}
        )

        records_av_stock = av_stock_data.to_dict(orient="records")
        av_stock_raw = [AvailableStock(**item) for item in records_av_stock]

        for i in range(0, len(av_stock_raw), BATCH_SIZE):
            batch = av_stock_raw[i : i + BATCH_SIZE]
            rows = list(batch)
            await AvailableStock.insert().add(*rows).run()
            await AvailableStock.insert().add(*rows).run(node="DB_2")

        # await AvailableStock.insert().add(*av_stock_raw).run()
        print(f"Вставлено {len(records_av_stock)} записей в AvailableStock.")
    else:
        print("DataFrame для AvailableStock пуст, пропускаем вставку.")

    # Пример:
    if not df_remains.empty:
        await Remains.delete(force=True).run()
        await Remains.delete(force=True).run(node="DB_2")
        remains_data = df_remains.merge(
            product_guide, on="product", how="left", suffixes=("_av", "_guide")
        )
        remains_data = remains_data.drop(
            ["product", "line_of_business_guide", "active_substance_guide"], axis=1
        )
        remains_data = remains_data.rename(columns={"id": "product"})
        remains_data = remains_data.rename(
            columns={"active_substance_av": "active_substance"}
        )
        remains_data = remains_data.rename(
            columns={"line_of_business_av": "line_of_business"}
        )
        records_remains = remains_data.to_dict(orient="records")
        remains_raw = [Remains(**item) for item in records_remains]
        for i in range(0, len(remains_raw), BATCH_SIZE):
            batch = remains_raw[i : i + BATCH_SIZE]
            rows = list(batch)
            await Remains.insert().add(*rows).run()
            await Remains.insert().add(*rows).run(node="DB_2")
        # await Remains.insert(*[Remains(**d) for d in records_remains]).run()
        print(f"Вставлено {len(records_remains)} записей в Remains.")
    else:
        print("DataFrame для Remains пуст, пропускаем вставку.")

    if not df_submissions.empty:
        await Submissions.delete(force=True).run()
        await Submissions.delete(force=True).run(node="DB_2")
        submissions_data = df_submissions.merge(
            product_guide, on="product", how="left", suffixes=("_av", "_guide")
        )
        submissions_data = submissions_data.drop(
            ["product", "line_of_business_guide", "active_substance"], axis=1
        )
        submissions_data = submissions_data.rename(columns={"id": "product"})
        submissions_data = submissions_data.rename(
            columns={"line_of_business_av": "line_of_business"}
        )
        records_submissions = submissions_data.to_dict(orient="records")
        submissions_raw = [Submissions(**item) for item in records_submissions]
        for i in range(0, len(submissions_raw), BATCH_SIZE):
            batch = submissions_raw[i : i + BATCH_SIZE]
            rows = list(batch)
            await Submissions.insert().add(*rows).run()
            await Submissions.insert().add(*rows).run(node="DB_2")
        # await Submissions.insert(*[Submissions(**d) for d in records_submissions]).run()
        print(f"Вставлено {len(records_submissions)} записей в Submissions.")
    else:
        print("DataFrame для Submissions пуст, пропускаем вставку.")

    if not df_payment.empty:
        await Payment.delete(force=True).run()
        await Payment.delete(force=True).run(node="DB_2")
        records_payment = df_payment.to_dict(orient="records")
        payment_raw = [Payment(**item) for item in records_payment]
        for i in range(0, len(payment_raw), BATCH_SIZE):
            batch = payment_raw[i : i + BATCH_SIZE]
            rows = list(batch)
            await Payment.insert().add(*rows).run()
            await Payment.insert().add(*rows).run(node="DB_2")
        # await Payment.insert(*[Payment(**d) for d in records_payment]).run()
        print(f"Вставлено {len(records_payment)} записей в Payment.")
    else:
        print("DataFrame для Payment пуст, пропускаем вставку.")

    if not df_moved.empty:
        await MovedData.delete(force=True).run()
        await MovedData.delete(force=True).run(node="DB_2")
        moved_data = df_moved.merge(
            product_guide, on="product", how="left", suffixes=("_av", "_guide")
        )
        moved_data["id"] = moved_data["id"].astype(str)
        moved_data = moved_data.drop(
            ["line_of_business_guide", "active_substance"], axis=1
        )
        moved_data = moved_data.rename(
            columns={"line_of_business_av": "line_of_business"}
        )
        moved_data = moved_data.rename(columns={"id": "product_id"})
        records_moved = moved_data.to_dict(orient="records")
        moved_raw = [MovedData(**item) for item in records_moved]
        for i in range(0, len(moved_raw), BATCH_SIZE):
            batch = moved_raw[i : i + BATCH_SIZE]
            rows = list(batch)
            await MovedData.insert().add(*rows).run()
            await MovedData.insert().add(*rows).run(node="DB_2")
        # await MovedData.insert(*[MovedData(**d) for d in records_moved]).run()
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
