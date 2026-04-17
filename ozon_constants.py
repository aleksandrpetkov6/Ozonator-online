from __future__ import annotations

import os
from pathlib import Path

APP_TITLE = "Store Economics — Ozon"
DEFAULT_BASE_URL = "https://api-seller.ozon.ru"
MOSCOW_OFFSET = "+03:00"
MAX_REQUEST_DAYS = 14
MAX_FINANCE_DAYS = 31
FBS_LIST_PATH = "/v3/posting/fbs/list"
FBO_LIST_PATH = "/v2/posting/fbo/list"
FINANCE_TRANSACTION_LIST_PATH = "/v3/finance/transaction/list"
REPORT_POSTINGS_CREATE_PATH = "/v1/report/postings/create"
REPORT_INFO_PATH = "/v1/report/info"
PRODUCT_INFO_LIST_PATH = "/v3/product/info/list"

APP_DIR = Path(os.getenv("APPDATA", str(Path.home()))) / "Store-Economics"
SETTINGS_PATH = APP_DIR / "settings.json"
ORDERS_CACHE_PATH = APP_DIR / "orders_cache.json.gz"
ECONOMY_CACHE_PATH = APP_DIR / "economy_cache.json.gz"

PAGE_SIZE = 3000
MONTH_NAMES = ["", "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь", "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"]
WEEKDAY_NAMES = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]

ORDERS_COLUMNS = [
    "Схема", "Номер отправления", "Связанные отправления", "Номер заказа",
    "Принят в обработку", "Дата отгрузки", "Статус", "Подстатус",
    "Предыдущий подстатус", "Детали перевозчика по статусу", "Регион доставки",
    "Город доставки", "Склад отгрузки", "Способ доставки", "Перевозчик",
    "Имя получателя", "Телефон получателя", "SKU", "Артикул", "Название товара",
    "Количество", "Ваша цена", "Код валюты товара", "Сумма отправления",
    "Цена товара до скидок", "Скидка %", "Скидка руб", "Акции", "Выкуп товара",
    "Кластер отгрузки", "Кластер доставки", "Верхний штрихкод", "Нижний штрихкод",
]

ECONOMY_COLUMNS = [
    "Дата операции", "Тип транзакции", "Тип операции", "Операция", "Номер операции",
    "Номер отправления", "Номер заказа", "Схема", "SKU", "Артикул", "Название товара",
    "Количество", "Начисление за продажу", "Комиссия", "Доставка", "Возврат доставки",
    "Услуги", "Список услуг", "Итого", "Валюта",
]

ALL_COLUMNS_BY_TAB = {"orders": ORDERS_COLUMNS, "economy": ECONOMY_COLUMNS}
CACHE_BY_TAB = {"orders": ORDERS_CACHE_PATH, "economy": ECONOMY_CACHE_PATH}
TAB_TITLES = {"orders": "Заказы", "economy": "Экономика магазина"}

STATUS_TRANSLATIONS = {
    "awaiting_registration": "Ожидает регистрации",
    "acceptance_in_progress": "Приёмка в процессе",
    "awaiting_approve": "Ожидает подтверждения",
    "awaiting_packaging": "Ожидает упаковки",
    "awaiting_deliver": "Ожидает отгрузки",
    "arbitration": "Арбитраж",
    "client_arbitration": "Клиентский арбитраж",
    "delivering": "Доставляется",
    "driver_pickup": "Забор водителем",
    "delivered": "Доставлено",
    "cancelled": "Отменено",
    "canceled": "Отменено",
}
SUBSTATUS_TRANSLATIONS = {
    "posting_received": "Получено",
    "posting_created": "Создано",
    "posting_canceled": "Отменено",
    "posting_cancelled": "Отменено",
    "posting_in_carriage": "В перевозке",
    "posting_transferring_to_delivery": "Передаётся в доставку",
    "posting_awaiting_passport_data": "Ожидает паспортные данные",
    "posting_delivered": "Доставлено",
    "posting_delivering": "Доставляется",
    "posting_last_mile": "Последняя миля",
    "posting_acceptance_in_progress": "Приёмка в процессе",
    "posting_awaiting_deliver": "Ожидает отгрузки",
    "posting_awaiting_packaging": "Ожидает упаковки",
    "posting_returned": "Возвращено",
}
DELIVERY_TYPE_TRANSLATIONS = {
    "Courier": "Курьер", "PVZ": "ПВЗ", "Postamat": "Постамат",
    "Pick-up point": "ПВЗ", "Courier delivery": "Курьерская доставка", "self": "Самовывоз",
}
CURRENCY_TRANSLATIONS = {"RUB": "руб.", "RUR": "руб.", "USD": "долл.", "EUR": "евро", "CNY": "юань"}
SCHEMA_TRANSLATIONS = {"FBS": "ФБС", "FBO": "ФБО", "RFBS": "рФБС", "fbs": "ФБС", "fbo": "ФБО", "rfbs": "рФБС"}
TRANSACTION_TYPE_TRANSLATIONS = {
    "all": "Все", "orders": "Заказы", "returns": "Возвраты", "services": "Услуги",
    "compensation": "Компенсации", "other": "Прочее",
}
OPERATION_TYPE_TRANSLATIONS = {
    "OperationAgentDeliveredToCustomer": "Доставка покупателю",
    "OperationReturnGoods": "Возврат товара",
    "OperationMarketplaceServiceStorage": "Хранение",
    "OperationMarketplaceServiceItemDirectFlowTrans": "Магистраль",
    "OperationMarketplaceServiceItemDelivToCustomer": "Доставка до покупателя",
    "OperationMarketplaceServiceItemReturnFlowTrans": "Обратная магистраль",
    "OperationMarketplaceServiceItemReturnAfterDelivToCustomer": "Возврат после доставки",
    "OperationMarketplaceServiceItemDropoffFF": "Обработка отправления",
    "OperationMarketplaceServiceItemDropoffPVZ": "Обработка отправления",
    "OperationMarketplaceServicePremiumCashback": "Премиум-кешбэк",
    "OperationClaim": "Начисление по претензии",
    "OperationDefectiveWriteOff": "Списание брака",
    "ClientReturnAgentOperation": "Клиентский возврат",
    "MarketplaceSale": "Продажа",
}
SERVICE_NAME_TRANSLATIONS = {
    "MarketplaceServiceItemFulfillment": "Фулфилмент",
    "MarketplaceServiceItemPickup": "Приёмка",
    "MarketplaceServiceItemDropoffPVZ": "Обработка отправления",
    "MarketplaceServiceItemDropoffFF": "Обработка отправления",
    "MarketplaceServiceItemDelivery": "Доставка",
    "MarketplaceServiceItemReturn": "Возврат",
}

TECH_WORD_TRANSLATIONS = {
    "posting": "отправление",
    "received": "получено",
    "created": "создано",
    "canceled": "отменено",
    "cancelled": "отменено",
    "cancel": "отмена",
    "delivered": "доставлено",
    "delivering": "доставляется",
    "delivery": "доставка",
    "deliver": "доставка",
    "awaiting": "ожидает",
    "approve": "подтверждение",
    "approval": "подтверждение",
    "packaging": "упаковка",
    "registration": "регистрация",
    "acceptance": "приёмка",
    "progress": "в процессе",
    "transferring": "передаётся",
    "carrier": "перевозчик",
    "carriage": "перевозка",
    "driver": "водитель",
    "pickup": "забор",
    "last": "последняя",
    "mile": "миля",
    "return": "возврат",
    "returned": "возвращено",
    "arbitration": "арбитраж",
    "client": "клиент",
    "seller": "продавец",
    "passport": "паспортные",
    "data": "данные",
    "not": "не",
    "accepted": "принято",
    "failed": "ошибка",
    "unknown": "неизвестно",
    "warehouse": "склад",
    "courier": "курьер",
    "pvz": "ПВЗ",
    "postamat": "постамат",
    "premium": "премиум",
    "legal": "юрлицо",
    "cash": "наличные",
    "card": "карта",
    "paid": "оплачено",
    "cashback": "кешбэк",
    "unpaid": "не оплачено",
    "rfbs": "рФБС",
    "fbs": "ФБС",
    "fbo": "ФБО",
}

# Дополнительные точные переводы значений, которые часто приходят в FBS/FBO-ответах Ozon.
TECH_EXACT_TRANSLATIONS = {
    "posting_received": "Получено",
    "posting_created": "Создано",
    "posting_canceled": "Отменено",
    "posting_cancelled": "Отменено",
    "posting_in_carriage": "В перевозке",
    "posting_transferring_to_delivery": "Передаётся в доставку",
    "posting_awaiting_passport_data": "Ожидает паспортные данные",
    "posting_delivered": "Доставлено",
    "posting_delivering": "Доставляется",
    "posting_last_mile": "Последняя миля",
    "posting_acceptance_in_progress": "Приёмка в процессе",
    "posting_awaiting_deliver": "Ожидает отгрузки",
    "posting_awaiting_packaging": "Ожидает упаковки",
    "posting_returned": "Возвращено",
    "awaiting_registration": "Ожидает регистрации",
    "acceptance_in_progress": "Приёмка в процессе",
    "awaiting_approve": "Ожидает подтверждения",
    "awaiting_packaging": "Ожидает упаковки",
    "awaiting_deliver": "Ожидает отгрузки",
    "arbitration": "Арбитраж",
    "client_arbitration": "Клиентский арбитраж",
    "delivering": "Доставляется",
    "delivered": "Доставлено",
    "cancelled": "Отменено",
    "canceled": "Отменено",
    "driver_pickup": "Забор водителем",
    "sent_by_seller": "Отправлено продавцом",
    "not_accepted": "Не принято",
    "awaiting_verification": "Ожидает проверки",
}
MONEY_COLUMNS = {
    "Ваша цена", "Сумма отправления", "Цена товара до скидок", "Скидка руб",
    "Начисление за продажу", "Комиссия", "Доставка", "Возврат доставки", "Услуги", "Итого",
}
DATETIME_COLUMNS = {"Принят в обработку", "Дата отгрузки"}
DATE_ONLY_COLUMNS = {"Дата операции"}


