import asyncio
import logging
from datetime import datetime

from new_agri_bot_backend.tables import Deliveries, DeliveryItems, Submissions
def get_fallback_weight(line_of_business: str, nomenclature: str) -> float:
    LOB_WEIGHT_MAP = {
        "Власне виробництво насіння": 1.0,
        "ЗЗР": 1.2,
        "Міндобрива (основні)": 1000.0,
    }

    if line_of_business in LOB_WEIGHT_MAP:
        return LOB_WEIGHT_MAP[line_of_business]

    if line_of_business == "Насіння":
        nom = nomenclature or ""
        if "(1500К)" in nom:
            return 8.0
        if "(150К)" in nom:
            return 10.0
        if "(50К)" in nom:
            return 15.0
        if "(80К)" in nom:
            return 20.0

    return 1.0

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

async def run():
    # 1. Создаем бэкап таблицы Deliveries
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_table_name = f"deliveries_backup_{timestamp}"
    
    logger.info(f"=== Создание бэкапа таблицы deliveries -> {backup_table_name} ===")
    try:
        await Deliveries.raw(f"CREATE TABLE {backup_table_name} AS SELECT * FROM deliveries;")
        logger.info(f"Бэкап успешно создан! Таблица: {backup_table_name}")
    except Exception as e:
        logger.error(f"Ошибка при создании бэкапа: {e}")
        logger.error("Остановка скрипта для предотвращения потери данных.")
        return

    # 2. Ищем доставки с нулевым или пустым весом
    logger.info("=== Поиск доставок с нулевым весом ===")
    deliveries = await Deliveries.select().where(
        (Deliveries.total_weight == 0) | (Deliveries.total_weight.is_null())
    ).run()

    logger.info(f"Найдено заявок с нулевым весом: {len(deliveries)}")

    updated_count = 0
    for delivery in deliveries:
        items = await DeliveryItems.select().where(DeliveryItems.delivery == delivery['id']).run()
        if not items:
            logger.info(f"Доставка ID {delivery['id']} не содержит товаров, пропускаем.")
            continue
        
        total_weight = 0.0
        
        for item in items:
            product_str = item.get('product') or ""
            order_ref = item.get('order_ref') or ""
            quantity = item.get('quantity') or 0.0
            
            # Ищем номенклатуру и LOB через Submissions
            sub_matches = await Submissions.select().where(Submissions.contract_supplement == order_ref).run()
            
            lob = ""
            nom = ""
            
            # Поиск точного или частичного совпадения
            for sub in sub_matches:
                sub_nom = sub.get('nomenclature') or ""
                if sub_nom in product_str:
                    lob = sub.get('line_of_business') or ""
                    nom = sub_nom
                    break
            
            # Простая эвристика, если через Submissions найти не удалось
            if not lob:
                if "Насіння" in product_str:
                    lob = "Насіння"
                elif "ЗЗР" in product_str:
                    lob = "ЗЗР"
                elif "Добрива" in product_str or "Міндобрива" in product_str:
                    lob = "Міндобрива (основні)"
            
            # Применяем fallback-логику
            fallback_w = get_fallback_weight(lob, nom or product_str)
            total_weight += quantity * fallback_w
            logger.debug(f"Товар: {product_str}, Кол-во: {quantity}, Расчетный вес 1 шт: {fallback_w}")
        
        # Обновляем вес
        if total_weight > 0:
            await Deliveries.update({Deliveries.total_weight: total_weight}).where(Deliveries.id == delivery['id']).run()
            logger.info(f"Доставка ID {delivery['id']} обновлена: установлен вес {total_weight} кг")
            updated_count += 1
            
    logger.info(f"=== Готово. Успешно обновлено доставок: {updated_count} ===")

if __name__ == "__main__":
    asyncio.run(run())
