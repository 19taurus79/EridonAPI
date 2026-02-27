import csv
from collections import defaultdict
import asyncio
from typing import Optional

from piccolo.query import Insert

from new_agri_bot_backend.tables import AddressGuide
from new_agri_bot_backend.config import logger


def _get_pk_code(row: dict) -> Optional[str]:
    """
    Вычисляет уникальный код (Primary Key) для текущей строки,
    выбирая код самого низкого заполненного уровня.
    """
    if row.get("level_5_id"):
        return row["level_5_id"]
    if row.get("level_4_id"):
        return row["level_4_id"]
    if row.get("level_3_id"):
        return row["level_3_id"]
    if row.get("level_2_id"):
        return row["level_2_id"]
    if row.get("level_1_id"):
        return row["level_1_id"]
    return None


async def load_address_guide_data(csv_filepath: str):
    """
    Читает CSV, сортирует данные по иерархии и выполняет массовую вставку.
    """
    # ⚠️ ВАЖНО: Убедитесь, что этот список соответствует порядку зависимости!
    LOAD_ORDER = ["O", "K", "P", "H", "M", "X", "C", "B"]
    
    grouped_data = defaultdict(list)

    try:
        # 1. Чтение CSV, очистка и деривация Primary Key
        # Используем 'utf-8-sig', чтобы корректно обработать BOM-символ (\ufeff) в начале файла.
        with open(csv_filepath, "r", encoding="utf-8-sig") as f:
            # DictReader автоматически использует первую строку как заголовки (ключи словаря).
            reader = csv.DictReader(f, delimiter=';')

            for row in reader:
                # Очищаем значения от лишних пробелов.
                cleaned_row = {k: v.strip() if v else None for k, v in row.items()}

                # --- ВЫЧИСЛЕНИЕ УНИКАЛЬНОГО КОДА (PK) ---
                pk_code = _get_pk_code(cleaned_row)

                if not pk_code:
                    # Если для строки не удалось определить уникальный код, пропускаем ее.
                    # (Это может произойти для пустых строк в конце файла)
                    continue

                # Если pk_code найден, присваиваем его и добавляем строку в нужную группу.
                cleaned_row["id"] = pk_code
                category = cleaned_row.get("category")

                if category and category in LOAD_ORDER:
                    grouped_data[category].append(cleaned_row)

    except FileNotFoundError:
        logger.error(f"❌ Ошибка: Файл '{csv_filepath}' не найден.")
        return
    except Exception as e:
        logger.error(f"❌ Ошибка чтения CSV: {e}")
        return

    # 2. Последовательная загрузка данных (для удовлетворения FK)
    for category in LOAD_ORDER:
        rows_to_insert = grouped_data.get(category, [])
        if not rows_to_insert:
            continue

        logger.info(f"--- Загрузка категории '{category}' ({len(rows_to_insert)} записей)...")

        try:
            # Создаем список экземпляров модели напрямую из словарей.
            models_to_insert = [
                AddressGuide(**row)
                for row in rows_to_insert
            ]

            # 3. Выполняем простую массовую вставку ПАКЕТАМИ.
            # ⚠️ ПРЕДПОЛАГАЕТСЯ, ЧТО ТАБЛИЦА БЫЛА ОЧИЩЕНА ВРУЧНУЮ ПЕРЕД ЗАПУСКОМ.
            BATCH_SIZE = 1000

            for i in range(0, len(models_to_insert), BATCH_SIZE):
                # Берем "срез" данных размером BATCH_SIZE
                batch = models_to_insert[i : i + BATCH_SIZE]
                # Вставляем только этот пакет
                await AddressGuide.insert(*batch).run()

            logger.info(
                f"✅ Успешно загружено {len(rows_to_insert)} записей категории '{category}'."
            )

        except Exception as e:
            logger.error(f"❌ Критическая ошибка при загрузке категории {category}: {e}")
            logger.error(
                "❗ Процесс остановлен. Убедитесь, что все родительские элементы загружены."
            )
            return


# ----------------------------------------------------

if __name__ == "__main__":
    # ⚠️ ЗАМЕНИТЕ ЭТУ СТРОКУ НА АКТУАЛЬНЫЙ ПУТЬ К ВАШЕМУ ФАЙЛУ
    CSV_FILE_PATH = "../Книга1.csv"

    import asyncio, os, sys

    # Добавляем путь к корневой папке проекта, чтобы импортировать piccolo_conf
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
    from piccolo_conf import DB

    # Перед запуском убедитесь, что:
    # 1. Таблица AddressGuide создана миграцией.
    # 2. Конфигурация базы данных Piccolo настроена.

    async def run_main():
        try:
            await DB.start_connection_pool()
            await load_address_guide_data(CSV_FILE_PATH)
        finally:
            await DB.close_connection_pool()

    asyncio.run(run_main())
    logger.info("\n🏁 Загрузка завершена.")
