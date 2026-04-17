from __future__ import annotations

from collections import Counter
from typing import Any

from ozon_constants import (
    ORDERS_COLUMNS,
    ECONOMY_COLUMNS,
    OPERATION_TYPE_TRANSLATIONS,
    SERVICE_NAME_TRANSLATIONS,
)
from ozon_utils import (
    bool_label,
    extract_processing_date,
    extract_shipment_date,
    format_money,
    join_values,
    money_sum,
    normalize_row_values,
    safe_get,
    second_dash_prefix,
    to_decimal,
    translate_value,
)


def financial_products_index(posting: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for item in safe_get(posting, "financial_data.products", []) or []:
        product_id = item.get("product_id") or item.get("sku")
        if product_id is not None:
            out[str(product_id)] = item
    return out


def _override_shipment_date(posting: dict[str, Any], shipment_dates: dict[str, str] | None) -> str:
    posting_number = str(posting.get("posting_number") or "").strip()
    if shipment_dates and posting_number:
        value = shipment_dates.get(posting_number)
        if value:
            return value
    return ""


def common_posting_fields(
    posting: dict[str, Any],
    schema: str,
    posting_sum: str,
    shipment_dates: dict[str, str] | None = None,
) -> dict[str, Any]:
    delivery_method = posting.get("delivery_method") or {}
    analytics_data = posting.get("analytics_data") or {}
    financial_data = posting.get("financial_data") or {}
    barcodes = posting.get("barcodes") or {}
    warehouse = (
        safe_get(delivery_method, "warehouse")
        or analytics_data.get("warehouse_name")
        or analytics_data.get("warehouse")
        or financial_data.get("cluster_from")
        or ""
    )
    shipment_date = _override_shipment_date(posting, shipment_dates) or extract_shipment_date(posting)
    return normalize_row_values({
        "Схема": schema,
        "Номер отправления": posting.get("posting_number", ""),
        "Связанные отправления": "",
        "Номер заказа": posting.get("order_number", ""),
        "Принят в обработку": extract_processing_date(posting),
        "Дата отгрузки": shipment_date,
        "Статус": posting.get("status", ""),
        "Подстатус": posting.get("substatus", ""),
        "Предыдущий подстатус": posting.get("previous_substatus", ""),
        "Детали перевозчика по статусу": posting.get("provider_status", ""),
        "Регион доставки": analytics_data.get("region", ""),
        "Город доставки": analytics_data.get("city", ""),
        "Склад отгрузки": warehouse,
        "Способ доставки": analytics_data.get("delivery_type", ""),
        "Перевозчик": delivery_method.get("tpl_provider", ""),
        "Имя получателя": safe_get(posting, "addressee.name", ""),
        "Телефон получателя": safe_get(posting, "addressee.phone", ""),
        "Сумма отправления": posting_sum,
        "Кластер отгрузки": financial_data.get("cluster_from", ""),
        "Кластер доставки": financial_data.get("cluster_to", ""),
        "Верхний штрихкод": barcodes.get("upper_barcode", ""),
        "Нижний штрихкод": barcodes.get("lower_barcode", ""),
    }, ORDERS_COLUMNS)


def product_fields(product: dict[str, Any], financial_product: dict[str, Any]) -> dict[str, Any]:
    return normalize_row_values({
        "SKU": product.get("sku", ""),
        "Артикул": product.get("offer_id", ""),
        "Название товара": product.get("name", ""),
        "Количество": product.get("quantity", ""),
        "Ваша цена": product.get("price", ""),
        "Код валюты товара": product.get("currency_code", ""),
        "Цена товара до скидок": financial_product.get("old_price", ""),
        "Скидка %": financial_product.get("total_discount_percent", ""),
        "Скидка руб": financial_product.get("total_discount_value", ""),
        "Акции": join_values(financial_product.get("actions") or []),
        "Выкуп товара": bool_label(product.get("is_marketplace_buyout")),
    }, ORDERS_COLUMNS)


def empty_product_fields() -> dict[str, Any]:
    return {
        "SKU": "", "Артикул": "", "Название товара": "", "Количество": "", "Ваша цена": "",
        "Код валюты товара": "", "Цена товара до скидок": "", "Скидка %": "", "Скидка руб": "",
        "Акции": "", "Выкуп товара": "",
    }


def normalize_postings(
    postings: list[dict[str, Any]],
    schema: str,
    shipment_dates: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for posting in postings:
        products = posting.get("products") or []
        financial = financial_products_index(posting)
        common = common_posting_fields(posting, schema, money_sum(products), shipment_dates)
        if not products:
            rows.append({**common, **empty_product_fields()})
        for product in products:
            product_id = product.get("product_id") or product.get("sku")
            rows.append({**common, **product_fields(product, financial.get(str(product_id), {}))})
    return rows


def add_related_postings(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts = Counter(p for p in (second_dash_prefix(r.get("Номер отправления")) for r in rows) if p)
    for row in rows:
        prefix = second_dash_prefix(row.get("Номер отправления"))
        row["Связанные отправления"] = prefix if prefix and counts[prefix] > 1 else ""
    return rows


def build_offer_map_from_rows(rows: list[dict[str, Any]]) -> dict[str, str]:
    out: dict[str, str] = {}
    for row in rows:
        sku = str(row.get("SKU") or "").strip()
        offer_id = str(row.get("Артикул") or "").strip()
        if sku and offer_id and sku not in out:
            out[sku] = offer_id
    return out


def _item_sku(item: dict[str, Any]) -> str:
    return str(item.get("sku") or item.get("product_id") or item.get("item_id") or "").strip()


def normalize_finance_operations(
    operations: list[dict[str, Any]],
    sku_offer_map: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    sku_offer_map = sku_offer_map or {}
    for operation in operations:
        posting = operation.get("posting") or {}
        items = operation.get("items") or []
        if not items:
            items = [{}]
        services = operation.get("services") or []
        service_sum = sum(to_decimal(s.get("price")) for s in services if isinstance(s, dict))
        service_names = []
        for s in services:
            if not isinstance(s, dict):
                continue
            name = s.get("name") or s.get("service_name") or ""
            price = format_money(s.get("price", ""))
            label = translate_value(name, SERVICE_NAME_TRANSLATIONS)
            service_names.append(f"{label}: {price}" if price else label)
        op_name = operation.get("operation_type_name") or operation.get("name") or operation.get("operation_name") or ""
        for item in items:
            if not isinstance(item, dict):
                item = {}
            sku = _item_sku(item)
            offer_id = str(item.get("offer_id") or item.get("offer") or item.get("vendor_code") or "").strip()
            if not offer_id and sku:
                offer_id = sku_offer_map.get(sku, "")
            row = {
                "Дата операции": operation.get("operation_date") or operation.get("date") or operation.get("created_at") or "",
                "Тип транзакции": operation.get("type") or operation.get("transaction_type") or "",
                "Тип операции": operation.get("operation_type") or "",
                "Операция": op_name or translate_value(operation.get("operation_type", ""), OPERATION_TYPE_TRANSLATIONS),
                "Номер операции": operation.get("operation_id") or operation.get("id") or "",
                "Номер отправления": posting.get("posting_number") or operation.get("posting_number") or "",
                "Номер заказа": posting.get("order_number") or operation.get("order_number") or "",
                "Схема": posting.get("delivery_schema") or operation.get("delivery_schema") or "",
                "SKU": sku,
                "Артикул": offer_id,
                "Название товара": item.get("name") or item.get("item_name") or "",
                "Количество": item.get("quantity") or (1 if item else ""),
                "Начисление за продажу": operation.get("accruals_for_sale", ""),
                "Комиссия": operation.get("sale_commission", ""),
                "Доставка": operation.get("delivery_charge", ""),
                "Возврат доставки": operation.get("return_delivery_charge", ""),
                "Услуги": str(service_sum),
                "Список услуг": "; ".join(x for x in service_names if x),
                "Итого": operation.get("amount", ""),
                "Валюта": operation.get("currency_code") or operation.get("currency") or "RUB",
            }
            rows.append(normalize_row_values(row, ECONOMY_COLUMNS))
    return rows
