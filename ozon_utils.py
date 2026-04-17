from __future__ import annotations

import gzip
import json
import re
from datetime import date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

from ozon_constants import *

def parse_date(value: str) -> date:
    text = (value or "").strip().replace("/", "-").replace(".", "-")
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d-%m-%y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            pass
    raise ValueError("Дата должна быть в формате YYYY-MM-DD или ДД.ММ.ГГГГ")


def date_to_ozon_datetime(value: date, *, end_of_day: bool = False) -> str:
    boundary = time(23, 59, 59) if end_of_day else time(0, 0, 0)
    return datetime.combine(value, boundary).isoformat() + MOSCOW_OFFSET


def date_chunks(start: date, end: date, max_days: int) -> list[tuple[date, date]]:
    chunks: list[tuple[date, date]] = []
    current = start
    while current <= end:
        chunk_end = min(current + timedelta(days=max_days - 1), end)
        chunks.append((current, chunk_end))
        current = chunk_end + timedelta(days=1)
    return chunks


def format_datetime(value: Any) -> str:
    if value in (None, "", "None"):
        return ""
    text = str(value).strip()
    if not text:
        return ""
    normalized = text.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
        return dt.strftime("%d.%m.%y %H:%M:%S")
    except ValueError:
        pass
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%d.%m.%Y %H:%M:%S", "%d.%m.%y %H:%M:%S", "%Y-%m-%d"):
        try:
            sample = text[:19] if "%H" in fmt else text[:10]
            return datetime.strptime(sample, fmt).strftime("%d.%m.%y %H:%M:%S")
        except ValueError:
            pass
    return text


def format_date_only(value: Any) -> str:
    if value in (None, "", "None"):
        return ""
    formatted = format_datetime(value)
    return formatted[:8] if len(formatted) >= 8 and formatted[2:3] == "." else formatted


def sort_key(value: Any) -> tuple[int, Any]:
    if value in (None, ""):
        return (1, "")
    text = str(value)
    for fmt in ("%d.%m.%y %H:%M:%S", "%d.%m.%Y %H:%M:%S"):
        try:
            return (0, datetime.strptime(text, fmt))
        except ValueError:
            pass
    try:
        return (0, Decimal(text.replace(" ", "").replace(",", ".").replace("руб.", "")))
    except Exception:
        return (0, text.lower())


def prettify_unknown(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return text.replace("_", " ").replace("-", " ")


def _has_latin_letters(text: str) -> bool:
    return bool(re.search(r"[A-Za-z]", text or ""))


def translate_technical_value(value: Any, mapping: dict[str, str] | None = None) -> str:
    """Переводит технические значения Ozon в русские подписи.

    Для неизвестных кодов не оставляет английские слова в таблице: разбирает код
    по словам и переводит знакомые части. Если код совсем неизвестный, показывает
    нейтральное русское значение.
    """
    text = str(value or "").strip()
    if not text or text == "None":
        return ""
    if mapping and text in mapping:
        return mapping[text]
    if text in TECH_EXACT_TRANSLATIONS:
        return TECH_EXACT_TRANSLATIONS[text]
    if not _has_latin_letters(text):
        return text
    parts = [p for p in re.split(r"[_\-\s/]+", text.lower()) if p]
    translated = [TECH_WORD_TRANSLATIONS.get(p, "") for p in parts]
    translated = [p for p in translated if p]
    if translated:
        result = " ".join(translated)
        return result[:1].upper() + result[1:]
    return "Не указано"


def translate_value(value: Any, mapping: dict[str, str]) -> str:
    text = str(value or "").strip()
    if not text or text == "None":
        return ""
    if text in mapping:
        return mapping[text]
    return translate_technical_value(text, mapping)


def format_money(value: Any) -> str:
    if value in (None, "", "None"):
        return ""
    try:
        amount = Decimal(str(value).replace(" ", "").replace(",", "."))
    except (InvalidOperation, ValueError):
        return str(value)
    if amount == amount.to_integral_value():
        return str(amount.quantize(Decimal("1")))
    return str(amount.quantize(Decimal("0.01")))


def format_number(value: Any) -> str:
    if value in (None, "", "None"):
        return ""
    try:
        amount = Decimal(str(value).replace(" ", "").replace(",", "."))
    except (InvalidOperation, ValueError):
        return str(value)
    if amount == amount.to_integral_value():
        return str(amount.quantize(Decimal("1")))
    return str(amount.normalize())


def safe_get(data: dict[str, Any] | None, path: str, default: Any = "") -> Any:
    current: Any = data or {}
    for part in path.split("."):
        if not isinstance(current, dict) or part not in current:
            return default
        current = current[part]
    return current


def to_decimal(value: Any) -> Decimal:
    if value in (None, ""):
        return Decimal("0")
    try:
        return Decimal(str(value).replace(",", "."))
    except (InvalidOperation, ValueError):
        return Decimal("0")


def money_sum(products: list[dict[str, Any]] | None) -> str:
    total = Decimal("0")
    for product in products or []:
        total += to_decimal(product.get("price")) * to_decimal(product.get("quantity") or 1)
    return str(total)


def join_values(values: Any) -> str:
    if not isinstance(values, list):
        return ""
    return ", ".join(str(v) for v in values if v not in (None, ""))


def second_dash_prefix(posting_number: Any) -> str:
    parts = str(posting_number or "").split("-")
    return "-".join(parts[:2]) if len(parts) >= 2 else ""


def bool_label(value: Any) -> str:
    return "да" if value is True else "нет" if value is False else ""


def normalize_row_values(row: dict[str, Any], columns: list[str]) -> dict[str, Any]:
    for col in columns:
        if col not in row:
            continue
        if col in DATETIME_COLUMNS:
            row[col] = format_datetime(row.get(col, ""))
        elif col in DATE_ONLY_COLUMNS:
            row[col] = format_date_only(row.get(col, ""))
        elif col in MONEY_COLUMNS:
            row[col] = format_money(row.get(col, ""))
    if "Статус" in row:
        row["Статус"] = translate_technical_value(row.get("Статус", ""), STATUS_TRANSLATIONS)
    if "Подстатус" in row:
        row["Подстатус"] = translate_technical_value(row.get("Подстатус", ""), SUBSTATUS_TRANSLATIONS)
    if "Предыдущий подстатус" in row:
        row["Предыдущий подстатус"] = translate_technical_value(row.get("Предыдущий подстатус", ""), SUBSTATUS_TRANSLATIONS)
    if "Детали перевозчика по статусу" in row:
        row["Детали перевозчика по статусу"] = translate_technical_value(row.get("Детали перевозчика по статусу", ""), SUBSTATUS_TRANSLATIONS)
    if "Способ доставки" in row:
        row["Способ доставки"] = translate_value(row.get("Способ доставки", ""), DELIVERY_TYPE_TRANSLATIONS)
    if "Код валюты товара" in row:
        row["Код валюты товара"] = translate_value(row.get("Код валюты товара", ""), CURRENCY_TRANSLATIONS)
    if "Валюта" in row:
        row["Валюта"] = translate_value(row.get("Валюта", ""), CURRENCY_TRANSLATIONS)
    if "Схема" in row:
        row["Схема"] = translate_value(row.get("Схема", ""), SCHEMA_TRANSLATIONS)
    if "Тип транзакции" in row:
        row["Тип транзакции"] = translate_value(row.get("Тип транзакции", ""), TRANSACTION_TYPE_TRANSLATIONS)
    if "Тип операции" in row:
        row["Тип операции"] = translate_value(row.get("Тип операции", ""), OPERATION_TYPE_TRANSLATIONS)
    return row


def save_rows_cache(tab_key: str, rows: list[dict[str, Any]]) -> None:
    APP_DIR.mkdir(parents=True, exist_ok=True)
    with gzip.open(CACHE_BY_TAB[tab_key], "wt", encoding="utf-8") as fh:
        json.dump(rows, fh, ensure_ascii=False)


def load_rows_cache(tab_key: str) -> list[dict[str, Any]]:
    path = CACHE_BY_TAB[tab_key]
    if not path.exists():
        return []
    try:
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            data = json.load(fh)
        if not isinstance(data, list):
            return []
        columns = ALL_COLUMNS_BY_TAB[tab_key]
        return [normalize_row_values(r, columns) for r in data if isinstance(r, dict)]
    except Exception:
        return []


def schema_label(value: Any) -> str:
    return translate_value(value, SCHEMA_TRANSLATIONS)


def _first_non_empty_from_paths(data: dict[str, Any], paths: list[str]) -> Any:
    for path in paths:
        value = safe_get(data, path, "") if "." in path else data.get(path, "")
        if value not in (None, "", "None"):
            return value
    return ""


def _looks_like_datetime_value(value: Any) -> bool:
    if value in (None, "", "None"):
        return False
    text = str(value)
    return bool(re.search(r"\d{4}-\d{2}-\d{2}|\d{2}\.\d{2}\.\d{2,4}", text))


def _find_nested_date_by_keywords(data: Any, keywords: tuple[str, ...]) -> Any:
    """Ищет дату во вложенном ответе Ozon по названию ключа.

    В разных схемах и версиях API дата может называться по-разному. Эта функция
    не заменяет точные поля, а только страхует случай, когда дата пришла в новом
    вложенном ключе.
    """
    if isinstance(data, dict):
        for key, value in data.items():
            key_l = str(key).lower()
            if any(word in key_l for word in keywords) and _looks_like_datetime_value(value):
                return value
        for value in data.values():
            found = _find_nested_date_by_keywords(value, keywords)
            if found not in (None, "", "None"):
                return found
    elif isinstance(data, list):
        for value in data:
            found = _find_nested_date_by_keywords(value, keywords)
            if found not in (None, "", "None"):
                return found
    return ""


def extract_processing_date(posting: dict[str, Any]) -> Any:
    value = _first_non_empty_from_paths(posting, [
        "in_process_at", "created_at", "creation_date", "posting_created_at", "processed_at",
        "analytics_data.in_process_at", "analytics_data.created_at", "financial_data.created_at",
    ])
    if value:
        return value
    return _find_nested_date_by_keywords(posting, ("process", "created", "creation"))


def extract_shipment_date(posting: dict[str, Any]) -> Any:
    # FBS обычно отдаёт delivering_date. У FBO поле может называться иначе или отсутствовать в posting API.
    value = _first_non_empty_from_paths(posting, [
        "delivering_date", "shipment_date", "shipment_date_at", "shipping_date", "sent_at", "send_at", "shipped_at",
        "transferring_to_delivery_at", "transferred_to_delivery_at", "delivery_date", "delivered_at",
        "analytics_data.delivering_date", "analytics_data.shipment_date", "analytics_data.shipping_date",
        "financial_data.delivery_date", "financial_data.shipment_date",
    ])
    if value:
        return value
    return _find_nested_date_by_keywords(posting, ("deliver", "shipment", "shipping", "sent", "shipped", "transfer"))


