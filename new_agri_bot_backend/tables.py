from datetime import datetime

from piccolo.columns.defaults import TimestampNow
from piccolo.columns.defaults.timestamptz import TimestamptzNow
from piccolo.columns.readable import Readable
from piccolo.table import Table
from piccolo.columns import (
    Varchar,
    UUID,
    ForeignKey,
    Date,
    DoublePrecision,
    BigInt,
    Boolean,
    Timestamptz,
    Numeric,
    Integer,
    Timestamp,
    Text,
    Real,
    JSONB,
    OnDelete,
)


class ProductGuide(Table):
    id = UUID(primary_key=True)
    product = Varchar(null=False)
    line_of_business = Varchar(null=True)
    active_substance = Varchar(null=True)


#


class Remains(Table):
    id = UUID(primary_key=True)
    line_of_business = Varchar(null=False)
    warehouse = Varchar(null=True)
    parent_element = Varchar(null=True)
    nomenclature = Varchar(null=True)
    party_sign = Varchar(null=True)
    buying_season = Varchar(null=True)
    nomenclature_series = Varchar(null=True)
    mtn = Varchar(null=True)
    origin_country = Varchar(null=True)
    germination = Varchar(null=True)
    crop_year = Varchar(null=True)
    quantity_per_pallet = Varchar(null=True)
    active_substance = Varchar(null=True)
    certificate = Varchar(null=True)
    certificate_start_date = Varchar(null=True)
    certificate_end_date = Varchar(null=True)
    buh = DoublePrecision()
    skl = DoublePrecision()
    weight = Varchar(null=True)
    storage = DoublePrecision()
    product = ForeignKey(references=ProductGuide)


class Submissions(Table):
    id = UUID(primary_key=True)
    division = Varchar(null=True)
    manager = Varchar(null=True)
    company_group = Varchar(null=True)
    client = Varchar(null=True)
    contract_supplement = Varchar(null=True)
    parent_element = Varchar(null=True)
    manufacturer = Varchar(null=True)
    active_ingredient = Varchar(null=True)
    nomenclature = Varchar(null=True)
    party_sign = Varchar(null=True)
    buying_season = Varchar(null=True)
    line_of_business = Varchar(null=True)
    period = Varchar(null=True)
    shipping_warehouse = Varchar(null=True)
    document_status = Varchar(null=True)
    delivery_status = Varchar(null=True)
    shipping_address = Varchar(null=True)
    transport = Varchar(null=True)
    plan = DoublePrecision()
    fact = DoublePrecision()
    different = DoublePrecision()
    product = ForeignKey(references=ProductGuide)


class AvailableStock(Table):
    id = UUID(primary_key=True)
    nomenclature = Varchar(null=True)
    party_sign = Varchar(null=True)
    buying_season = Varchar(null=True)
    division = Varchar(null=True)
    line_of_business = Varchar(null=True)
    available = DoublePrecision()
    product = ForeignKey(references=ProductGuide)


class ProductUnderSubmissions(Table):
    id = UUID(primary_key=True)
    product = ForeignKey(references=ProductGuide)
    quantity = DoublePrecision()


class MovedData(Table):
    id = UUID(primary_key=True)
    product = Varchar()
    contract = Varchar(null=True)
    date = Date()
    line_of_business = Varchar()
    qt_order = DoublePrecision()
    qt_moved = DoublePrecision()
    party_sign = Varchar()
    period = Varchar()
    order = Varchar()
    product_id = Varchar()
    is_active = Boolean(default=True)


class MovedNot(Table):
    id = UUID(primary_key=True)
    product = Varchar()
    quantity = Varchar()
    contract = Varchar()
    note = Varchar()


class Payment(Table):
    id = UUID(primary_key=True)
    contract_supplement = Varchar()
    client = Varchar()
    contract_type = Varchar()
    order_status = Varchar()
    prepayment_amount = DoublePrecision()
    amount_of_credit = DoublePrecision()
    prepayment_percentage = DoublePrecision()
    loan_percentage = DoublePrecision()
    planned_amount = DoublePrecision()
    planned_amount_excluding_vat = DoublePrecision()
    actual_sale_amount = DoublePrecision()
    actual_payment_amount = DoublePrecision()


class Users(Table):
    telegram_id = BigInt(primary_key=True)
    username = Varchar()
    first_name = Varchar()
    last_name = Varchar()
    is_allowed = Boolean(default=False)
    registration_date = Timestamptz()
    last_activity_date = Timestamptz()
    is_admin = Boolean(default=False)
    full_name_for_orders = Varchar()


class ClientManagerGuide(Table):
    id = BigInt(primary_key=True)
    client = Varchar(required=True)
    manager = Varchar(required=True)

    class Meta:
        tablename = "client_manager_guide"


class ProductOnWarehouse(Table):
    id = BigInt(primary_key=True)
    product = Varchar()
    line_of_business = Varchar()

    class Meta:
        tablename = "product_on_warehouse"


class AvStockProd(Table):
    id = BigInt(primary_key=True)
    product = Varchar()
    line_of_business = Varchar()

    class Meta:
        tablename = "av_stock_prod"


class ProductsForOrders(Table):
    id = BigInt(primary_key=True)
    product = Varchar()
    order_q = DoublePrecision()
    remain_q = DoublePrecision()
    enough = Boolean(default=False)

    class Meta:
        tablename = "products_for_orders"


class DetailsForOrders(Table):
    nomenclature = Varchar(null=True)
    party_sign = Varchar(null=True)
    buying_season = Varchar(null=True)
    different = DoublePrecision()
    client = Varchar(null=True)
    contract_supplement = Varchar(null=True)
    manager = Varchar(null=True)
    product = UUID(required=True)
    orders_q = Numeric()
    moved_q = Numeric()
    party = Varchar()
    buh = Numeric()
    skl = Numeric()
    id = UUID(required=True)
    qok = Varchar(length=2)

    class Meta:
        tablename = "details_for_orders"

class ValidFreeStock(Table):
    product = Varchar()
    division = Varchar()
    warehouse = Varchar()
    free_qty = DoublePrecision()
    buh_qty = DoublePrecision()
    skl_qty = DoublePrecision()

    class Meta:
        tablename = "valid_free_stock"



class Events(Table):
    id = UUID(primary_key=True)
    event_id = Varchar()
    event_creator = BigInt()
    event_creator_name = Varchar()
    event_status = Integer()
    event_who_changed_id = BigInt()
    event_who_changed_name = Varchar()
    start_event = Date()
    event = Varchar()
    created_at = Timestamptz(default=TimestamptzNow())
    updated_at = Timestamp(auto_update=datetime.now)


class Tasks(Table):
    id = UUID(primary_key=True)
    task_id = Varchar()
    task_creator = BigInt()
    task_creator_name = Varchar()
    task_status = Integer()
    task_who_changed_id = BigInt()
    task_who_changed_name = Varchar()
    task = Varchar()
    chat_id = BigInt()
    message_id = BigInt()
    created_at = Timestamptz(default=TimestamptzNow())
    updated_at = Timestamp(auto_update=datetime.now)


class FreeStock(Table):
    id = UUID(primary_key=True)
    product = ForeignKey(references=ProductGuide)
    division = Varchar()
    warehouse = Varchar()
    date_in_co = Varchar()
    line_of_business = Varchar()
    free_qty = DoublePrecision()
    buh_qty = DoublePrecision()
    skl_qty = DoublePrecision()


class AddressGuide(Table):
    id = Varchar(primary_key=True)
    level_1_id = ForeignKey(references="AddressGuide")  # Область
    level_2_id = ForeignKey(references="AddressGuide")  # Район
    level_3_id = ForeignKey(references="AddressGuide")  # Громада
    level_4_id = ForeignKey(references="AddressGuide")  # Населенный пункт
    level_5_id = ForeignKey(references="AddressGuide")  # Район города
    category = Varchar()  # O, K, P, H, M, X, C, B
    name = Varchar()


class ClientAddress(Table):
    manager = Varchar()
    client = Varchar()
    region = Varchar()
    area = Varchar()
    commune = Varchar()
    city = Varchar()
    latitude = DoublePrecision()
    longitude = DoublePrecision()
    representative = Varchar()
    phone1 = Varchar()
    phone2 = Varchar()


# Таблица доставок
class Deliveries(Table):
    client = Varchar(length=255)
    manager = Varchar(length=255, null=True)
    address = Text(null=True)
    contact = Varchar(length=255, null=True)
    phone = Varchar(length=50, null=True)
    delivery_date = Date(null=True)
    comment = Text(null=True)
    total_weight = Real(null=True)
    is_custom_address = Boolean(default=False)
    latitude = Real(null=True)
    longitude = Real(null=True)
    created_by = BigInt(null=True)
    status = Varchar(length=50, default="Створено")
    created_at = Timestamp(default=TimestampNow())
    calendar_id = Varchar(null=True)


# Таблица товаров в доставке
class DeliveryItems(Table):
    delivery = ForeignKey(references=Deliveries, on_delete=OnDelete.cascade)
    order_ref = Varchar(length=255, null=True)
    product = Varchar(length=255)
    quantity = Real()
    party = Varchar(length=255, null=True)
    party_quantity = Real(null=True)


class OrderComments(Table):
    comment_type = Varchar(length=10)
    order_ref = Varchar(length=50)
    product_id = UUID()
    product_name = Varchar(length=255)
    comment_text = Text()
    created_by = BigInt()
    created_by_name = Varchar(length=100)
    created_at = Timestamp(default=TimestampNow())
    updated_at = Timestamp(auto_update=datetime.now)


class OrderChatMessage(Table):
    """Повідомлення чату для заявок"""

    id = UUID(primary_key=True)
    order_ref = Varchar(length=50, required=True, index=True)
    user_id = BigInt(required=True, index=True)
    user_name = Varchar(length=255, required=True)
    message_text = Text(required=True)
    created_at = Timestamptz(auto_now_add=True)
    updated_at = Timestamptz(auto_now=True)
    is_edited = Boolean(default=False)
    reply_to_message_id = ForeignKey(references="self", on_delete="SET NULL", null=True)

    @classmethod
    def get_readable(cls):
        return Readable(template="%s", columns=[cls.message_text])
