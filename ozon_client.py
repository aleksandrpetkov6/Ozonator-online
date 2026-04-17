from __future__ import annotations

import csv
import gzip
import io
import time as timer
import zipfile
from datetime import date
from typing import Any

import requests

from ozon_constants import (
    DEFAULT_BASE_URL,
    FBO_LIST_PATH,
    FBS_LIST_PATH,
    FINANCE_TRANSACTION_LIST_PATH,
    PRODUCT_INFO_LIST_PATH,
    REPORT_INFO_PATH,
    REPORT_POSTINGS_CREATE_PATH,
)
from ozon_utils import date_chunks, date_to_ozon_datetime


class OzonApiError(RuntimeError):
    pass


def _text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _decode_report_bytes(content: bytes) -> str:
    if content.startswith(b"\x1f\x8b"):
        content = gzip.decompress(content)
    elif content.startswith(b"PK\x03\x04"):
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            names = [name for name in zf.namelist() if not name.endswith("/")]
            if not names:
                return ""
            content = zf.read(names[0])
    for encoding in ("utf-8-sig", "cp1251", "utf-8"):
        try:
            return content.decode(encoding)
        except UnicodeDecodeError:
            continue
    return content.decode("utf-8", errors="replace")


def _detect_delimiter(first_line: str) -> str:
    return max([";", ",", "\t"], key=lambda ch: first_line.count(ch))


def _normalize_header(value: str) -> str:
    return _text(value).lower().replace("ё", "е").replace("(", " ").replace(")", " ").replace("-", " ").replace("_", " ")


def _pick(row: dict[str, str], aliases: list[str]) -> str:
    normalized = {_normalize_header(k): _text(v) for k, v in row.items()}
    for alias in aliases:
        value = normalized.get(_normalize_header(alias), "")
        if value:
            return value
    return ""


def _parse_report_datetime(value: Any) -> str:
    raw = _text(value)
    if not raw:
        return ""
    import re
    m = re.match(r"^(\d{2})\.(\d{2})\.(\d{4})(?:\s+(\d{2}):(\d{2})(?::(\d{2}))?)?$", raw)
    if m:
        day, month, year = m.group(1), m.group(2), m.group(3)
        hour, minute, second = m.group(4) or "00", m.group(5) or "00", m.group(6) or "00"
        return f"{year}-{month}-{day}T{hour}:{minute}:{second}+03:00"
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})(?:[ T](\d{2}):(\d{2})(?::(\d{2}))?)?", raw)
    if m:
        year, month, day = m.group(1), m.group(2), m.group(3)
        hour, minute, second = m.group(4) or "00", m.group(5) or "00", m.group(6) or "00"
        if raw.endswith("Z") or "+" in raw[10:] or "-" in raw[10:]:
            return raw
        return f"{year}-{month}-{day}T{hour}:{minute}:{second}+03:00"
    return raw


def _parse_report_csv(csv_text: str) -> list[dict[str, str]]:
    csv_text = csv_text.replace("\ufeff", "")
    first = csv_text.splitlines()[0] if csv_text.splitlines() else ""
    delimiter = _detect_delimiter(first)
    return [dict(row) for row in csv.DictReader(io.StringIO(csv_text), delimiter=delimiter)]


def _extract_shipment_dates_from_report(csv_text: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for row in _parse_report_csv(csv_text):
        posting_number = _pick(row, ["Номер отправления", "Отправление", "posting_number", "posting number"])
        if not posting_number:
            continue
        shipment_date = _parse_report_datetime(_pick(row, [
            "Фактическая дата передачи в доставку",
            "Фактическая дата передачи в доставку (МСК)",
            "Дата и время фактической передачи в доставку",
            "Фактическая дата отгрузки",
            "Фактическая дата передачи отправления в доставку",
            "Дата отгрузки",
            "shipment_date_actual",
            "shipment_date_fact",
            "shipment_date",
            "shipment date actual",
            "shipment date",
        ]))
        if shipment_date:
            current = out.get(posting_number)
            if not current or shipment_date > current:
                out[posting_number] = shipment_date
    return out


class OzonClient:
    def __init__(self, client_id: str, api_key: str, base_url: str = DEFAULT_BASE_URL):
        self.client_id = client_id.strip()
        self.api_key = api_key.strip()
        self.base_url = (base_url or DEFAULT_BASE_URL).strip().rstrip("/")
        if not self.client_id:
            raise ValueError("Не заполнен Client-Id / ID личного кабинета")
        if not self.api_key:
            raise ValueError("Не заполнен Api-Key / ключ API")
        self.session = requests.Session()
        self.session.headers.update({
            "Client-Id": self.client_id,
            "Api-Key": self.api_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
        })

    def post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            response = self.session.post(self.base_url + path, json=payload, timeout=120)
        except requests.RequestException as exc:
            raise OzonApiError(f"Не удалось подключиться к Ozon API: {exc}") from exc
        if response.status_code >= 400:
            try:
                details = response.json()
            except ValueError:
                details = response.text
            raise OzonApiError(f"Ozon API вернул ошибку {response.status_code}: {details}")
        try:
            return response.json()
        except ValueError as exc:
            raise OzonApiError("Ozon API вернул не JSON-ответ") from exc

    def list_postings(self, schema: str, since: str, to: str) -> list[dict[str, Any]]:
        path = FBS_LIST_PATH if schema == "FBS" else FBO_LIST_PATH
        with_payload = {"analytics_data": True, "financial_data": True}
        if schema == "FBS":
            with_payload.update({"barcodes": True, "translit": False})
        postings: list[dict[str, Any]] = []
        offset = 0
        limit = 1000
        while True:
            payload = {
                "dir": "ASC",
                "filter": {"since": since, "to": to},
                "limit": limit,
                "offset": offset,
                "with": with_payload,
            }
            data = self.post(path, payload)
            chunk = self._extract_postings(data)
            postings.extend(chunk)
            has_next = self._extract_has_next(data)
            if has_next is False or len(chunk) < limit:
                break
            offset += limit
        return postings

    def list_finance_transactions(self, since: str, to: str) -> list[dict[str, Any]]:
        operations: list[dict[str, Any]] = []
        page = 1
        page_size = 1000
        while True:
            payload = {
                "filter": {"date": {"from": since, "to": to}},
                "page": page,
                "page_size": page_size,
            }
            data = self.post(FINANCE_TRANSACTION_LIST_PATH, payload)
            chunk = self._extract_operations(data)
            operations.extend(chunk)
            result = data.get("result") if isinstance(data, dict) else {}
            page_count = result.get("page_count") if isinstance(result, dict) else None
            if page_count and page >= int(page_count):
                break
            if len(chunk) < page_size:
                break
            page += 1
        return operations

    def product_offer_map_by_skus(self, skus: list[str]) -> dict[str, str]:
        result: dict[str, str] = {}
        clean = []
        seen = set()
        for sku in skus:
            value = _text(sku)
            if value and value not in seen:
                clean.append(value)
                seen.add(value)
        for idx in range(0, len(clean), 1000):
            chunk = clean[idx:idx + 1000]
            payload_values = [int(x) if x.isdigit() else x for x in chunk]
            for field_name in ("sku", "product_id"):
                try:
                    data = self.post(PRODUCT_INFO_LIST_PATH, {field_name: payload_values})
                except Exception:
                    continue
                for item in self._extract_items(data):
                    offer_id = _text(item.get("offer_id") or item.get("offer") or item.get("vendor_code"))
                    if not offer_id:
                        continue
                    for key_name in ("sku", "fbo_sku", "fbs_sku", "product_id", "id"):
                        key = _text(item.get(key_name))
                        if key and key not in result:
                            result[key] = offer_id
        return result

    def shipment_dates_from_postings_report(self, delivery_schema: str, start_date: date, end_date: date) -> dict[str, str]:
        merged: dict[str, str] = {}
        # Метод из рабочего архива 1а: /v1/report/postings/create -> /v1/report/info -> CSV,
        # колонка «Фактическая дата передачи в доставку».
        for chunk_start, chunk_end in date_chunks(start_date, end_date, 7):
            try:
                partial = self._single_postings_report_shipment_dates(delivery_schema, chunk_start, chunk_end)
            except Exception:
                # Отчёт нужен для уточнения даты ФБО, но не должен ломать основную загрузку заказов.
                continue
            for posting_number, shipment_date in partial.items():
                if shipment_date and (posting_number not in merged or shipment_date > merged[posting_number]):
                    merged[posting_number] = shipment_date
        return merged

    def _single_postings_report_shipment_dates(self, delivery_schema: str, start_date: date, end_date: date) -> dict[str, str]:
        body = {
            "filter": {
                "processed_at_from": date_to_ozon_datetime(start_date),
                "processed_at_to": date_to_ozon_datetime(end_date, end_of_day=True),
                "delivery_schema": [delivery_schema.lower()],
            },
            "language": "DEFAULT",
        }
        created = self.post(REPORT_POSTINGS_CREATE_PATH, body)
        created_result = created.get("result", created) if isinstance(created, dict) else {}
        code = _text(created_result.get("code") if isinstance(created_result, dict) else "")
        if not code:
            return {}
        file_url = ""
        for _ in range(25):
            info = self.post(REPORT_INFO_PATH, {"code": code})
            info_result = info.get("result", info) if isinstance(info, dict) else {}
            if isinstance(info_result, dict):
                file_url = _text(info_result.get("file"))
                status = _text(info_result.get("status")).lower()
                if file_url:
                    break
                if status in {"error", "failed", "fail", "cancelled", "canceled"}:
                    return {}
            timer.sleep(1.5)
        if not file_url:
            return {}
        try:
            response = requests.get(file_url, timeout=120)
            response.raise_for_status()
        except requests.RequestException:
            return {}
        csv_text = _decode_report_bytes(response.content)
        return _extract_shipment_dates_from_report(csv_text)

    @staticmethod
    def _extract_postings(data: dict[str, Any]) -> list[dict[str, Any]]:
        result = data.get("result", data)
        if isinstance(result, dict):
            items = result.get("postings") or result.get("items") or []
        elif isinstance(result, list):
            items = result
        else:
            items = []
        return items if isinstance(items, list) else []

    @staticmethod
    def _extract_operations(data: dict[str, Any]) -> list[dict[str, Any]]:
        result = data.get("result", data)
        if isinstance(result, dict):
            items = result.get("operations") or result.get("items") or result.get("rows") or []
        elif isinstance(result, list):
            items = result
        else:
            items = []
        return items if isinstance(items, list) else []

    @staticmethod
    def _extract_items(data: dict[str, Any]) -> list[dict[str, Any]]:
        result = data.get("result", data)
        if isinstance(result, dict):
            items = result.get("items") or result.get("products") or []
        elif isinstance(result, list):
            items = result
        else:
            items = []
        return items if isinstance(items, list) else []

    @staticmethod
    def _extract_has_next(data: dict[str, Any]) -> bool | None:
        result = data.get("result")
        if isinstance(result, dict) and isinstance(result.get("has_next"), bool):
            return result["has_next"]
        if isinstance(data.get("has_next"), bool):
            return data["has_next"]
        return None
