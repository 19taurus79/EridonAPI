# Інструкція: Робота з Piccolo Admin (Адмін-панель)

Piccolo Admin — це вбудована панель керування базою даних, яка автоматично генерує зручний інтерфейс (CRUD) на основі ваших моделей (таблиць) з `tables.py`.

## 1. Як отримати доступ
- **Локально (Dev):** `http://localhost:8000/admin/`
- **Продакшен (Prod):** `https://<ваш_домен>/admin/`

> **Важливо:** Доступ регулюється масивом `allowed_hosts` у функції `create_admin` у файлі `main.py`. Наразі туди автоматично підтягуються всі домени з ваших налаштувань CORS, а також `localhost` та `127.0.0.1`.

## 2. Створення адміністратора
Логінів і паролів за замовчуванням немає. Щоб мати змогу увійти в адмінку, вам потрібно створити користувача через консоль (термінал):

```bash
# Переконайтесь, що ви знаходитесь у віртуальному середовищі
.venv\Scripts\piccolo.exe user create --username=admin --email=admin@eridon.com --password=ВАШ_ПАРОЛЬ --is_admin=True --is_superuser=True --is_active=True
```

*Примітка: якщо ви запускаєте це локально при відкритому SSH-туннелі (як описано в `LOCAL_DEV_DB_TUNNEL.md`), користувач буде створений одразу в **продакшен** базі даних.*

## 3. Як додати нову таблицю в адмінку

Усі налаштування адмінки знаходяться у файлі **`main.py`**.
Шукайте рядок `admin_router = create_admin(...)`.

### Просте додавання
Щоб таблиця з'явилась в інтерфейсі, достатньо її імпортувати з `tables.py` і передати в масив `tables`:

```python
from .tables import ValidWarehouseAdmin, Deliveries, Remains

admin_router = create_admin(
    tables=[Remains, ValidWarehouseAdmin, Deliveries],
    allowed_hosts=admin_allowed_hosts
)
```

## 4. Розширені налаштування таблиць (TableConfig)

Якщо ви хочете змінити те, як таблиця виглядає в адмінці (змінити назву, приховати зайві стовпці, згрупувати в меню), замість простої назви таблиці використовуйте `TableConfig`.

Вам потрібно імпортувати його:
```python
from piccolo_admin.endpoints import create_admin, TableConfig
```

І налаштувати таблицю:

```python
admin_router = create_admin(
    tables=[
        ValidWarehouseAdmin, # Звичайна таблиця
        
        # Налаштована таблиця
        TableConfig(
            table_class=Deliveries,
            name="Список доставок",                # Назва таблиці в інтерфейсі
            menu_group="Логістика",                 # Папка в лівому меню (Сайдбарі), де лежатиме таблиця
            visible_columns=[                       # Стовпці, які буде видно в загальному списку (таблиці)
                Deliveries.client, 
                Deliveries.status,
                Deliveries.delivery_date
            ],
            exclude_visible_columns=[               # Стовпці, які треба приховати із загального списку
                Deliveries.created_at,
                Deliveries.updated_at
            ],
            rich_text_columns=[                     # Поля, для яких треба увімкнути зручний текстовий редактор (WYSIWYG)
                Deliveries.comment
            ]
        )
    ],
    allowed_hosts=admin_allowed_hosts
)
```

## 5. Зрозуміле відображення зв'язків (Foreign Keys)

Якщо таблиця `A` посилається на таблицю `B` (Foreign Key), за замовчуванням в адмінці ви побачите довгий незрозумілий ID (наприклад, `a3f4b-22c1-...`).
Щоб замість ID показувалась назва (наприклад, ім'я клієнта або назва товару), у файлі **`tables.py`** всередині самої таблиці потрібно додати метод `get_readable()`.

Приклад:
```python
# tables.py
from piccolo.columns.readable import Readable
from piccolo.table import Table
from piccolo.columns import Varchar

class ProductGuide(Table):
    name = Varchar()
    category = Varchar()

    @classmethod
    def get_readable(cls):
        # При посиланні на цю таблицю показуватиметься колонка name (а не id)
        return Readable(template="%s", columns=[cls.name])
        
        # Можна навіть комбінувати кілька колонок:
        # return Readable(template="%s (%s)", columns=[cls.name, cls.category])
```

## 6. Що ще вміє Piccolo Admin "з коробки"
- **Фільтрація та сортування**: Натиснувши на будь-яку колонку в списку, ви можете фільтрувати дані (більше, менше, містить текст тощо).
- **Експорт в CSV**: У списку кожної таблиці є кнопка для вивантаження даних.
- **Масове видалення**: Можна виділити декілька записів галочками і видалити їх одним кліком.
- **Темна тема**: Перемикач знаходиться в налаштуваннях профілю (верхній правий кут).
