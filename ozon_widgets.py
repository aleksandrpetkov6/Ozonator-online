from __future__ import annotations

from calendar import monthrange
from datetime import date
from typing import Any

import tkinter as tk
from tkinter import ttk

from ozon_constants import PAGE_SIZE, MONTH_NAMES, WEEKDAY_NAMES
from ozon_utils import parse_date

class TableState:
    def __init__(self, key: str, columns: list[str]):
        self.key = key
        self.columns = columns
        self.rows: list[dict[str, Any]] = []
        self.filtered_rows: list[dict[str, Any]] = []
        self.visible_columns = columns.copy()
        self.column_widths: dict[str, int] = {}
        self.filters: dict[str, set[str]] = {}
        self.contains_filters: dict[str, str] = {}
        self.sort_column: str | None = None
        self.sort_reverse = False
        self.page = 0
        self.page_size = PAGE_SIZE
        self.tree: ttk.Treeview | None = None
        self.fill_after_id: str | None = None
        self.display_generation = 0


class DatePicker:
    def __init__(self, app: "DesktopApp", target_var: tk.StringVar):
        self.app = app
        self.root = app.root
        self.target_var = target_var
        try:
            initial = parse_date(target_var.get())
        except Exception:
            initial = date.today()
        self.year = initial.year
        self.month = initial.month
        self.selected = initial
        self.window = tk.Toplevel(self.root)
        self.window.title("Выбор даты")
        self.window.resizable(False, False)
        self.window.transient(self.root)
        self.window.grab_set()
        self.header_var = tk.StringVar()
        header = ttk.Frame(self.window, padding=10)
        header.grid(row=0, column=0, sticky="ew")
        ttk.Button(header, text="<", width=4, command=self.prev_month).grid(row=0, column=0)
        ttk.Label(header, textvariable=self.header_var, width=22, anchor="center", font=("Segoe UI", 10, "bold")).grid(row=0, column=1, padx=8)
        ttk.Button(header, text=">", width=4, command=self.next_month).grid(row=0, column=2)
        self.days_frame = ttk.Frame(self.window, padding=(10, 0, 10, 10))
        self.days_frame.grid(row=1, column=0)
        self.render()
        self.window.bind("<Escape>", lambda _event: self.window.destroy())
        self.center()

    def center(self) -> None:
        self.window.update_idletasks()
        x = self.root.winfo_x() + max(0, (self.root.winfo_width() - self.window.winfo_width()) // 2)
        y = self.root.winfo_y() + max(0, (self.root.winfo_height() - self.window.winfo_height()) // 2)
        self.window.geometry(f"+{x}+{y}")

    def prev_month(self) -> None:
        self.month -= 1
        if self.month == 0:
            self.month = 12
            self.year -= 1
        self.render()

    def next_month(self) -> None:
        self.month += 1
        if self.month == 13:
            self.month = 1
            self.year += 1
        self.render()

    def choose(self, day: int) -> None:
        self.target_var.set(date(self.year, self.month, day).isoformat())
        self.window.destroy()

    def render(self) -> None:
        for child in self.days_frame.winfo_children():
            child.destroy()
        self.header_var.set(f"{MONTH_NAMES[self.month]} {self.year}")
        for col, name in enumerate(WEEKDAY_NAMES):
            ttk.Label(self.days_frame, text=name, width=4, anchor="center").grid(row=0, column=col, padx=1, pady=1)
        first_weekday, days_count = monthrange(self.year, self.month)
        row = 1
        col = first_weekday
        for day in range(1, days_count + 1):
            current = date(self.year, self.month, day)
            text = f"[{day}]" if current == self.selected else str(day)
            ttk.Button(self.days_frame, text=text, width=4, command=lambda d=day: self.choose(d)).grid(row=row, column=col, padx=1, pady=1)
            col += 1
            if col == 7:
                col = 0
                row += 1
        ttk.Button(self.days_frame, text="Сегодня", command=self._today).grid(row=row + 1, column=0, columnspan=7, sticky="ew", pady=(8, 0))

    def _today(self) -> None:
        self.target_var.set(date.today().isoformat())
        self.window.destroy()


class FilterDialog:
    MAX_VALUES = 2500

    def __init__(self, app: "DesktopApp", table: TableState, column: str):
        self.app = app
        self.table = table
        self.column = column
        self.values = sorted({str(row.get(column, "")) for row in table.rows}, key=lambda x: x.lower())
        self.visible_values: list[str] = []
        self.search_var = tk.StringVar(value=table.contains_filters.get(column, ""))
        self.window = tk.Toplevel(app.root)
        self.window.title(f"Фильтр: {column}")
        self.window.transient(app.root)
        self.window.grab_set()
        self.window.geometry("460x520")
        self.window.minsize(390, 390)
        self.window.columnconfigure(0, weight=1)
        self.window.rowconfigure(3, weight=1)
        ttk.Label(self.window, text=column, font=("Segoe UI", 11, "bold")).grid(row=0, column=0, sticky="w", padx=12, pady=(12, 6))
        top = ttk.Frame(self.window)
        top.grid(row=1, column=0, sticky="ew", padx=12, pady=(0, 8))
        ttk.Button(top, text="Сорт ↑", command=lambda: self.sort_and_close(False)).grid(row=0, column=0, padx=(0, 6))
        ttk.Button(top, text="Сорт ↓", command=lambda: self.sort_and_close(True)).grid(row=0, column=1, padx=(0, 6))
        ttk.Button(top, text="Скрыть", command=self.hide_column).grid(row=0, column=2, padx=(0, 6))
        ttk.Button(top, text="Сброс", command=self.clear_filter).grid(row=0, column=3)
        search = ttk.Frame(self.window)
        search.grid(row=2, column=0, sticky="ew", padx=12, pady=(0, 8))
        search.columnconfigure(0, weight=1)
        self.search_entry = ttk.Entry(search, textvariable=self.search_var)
        self.search_entry.grid(row=0, column=0, sticky="ew", padx=(0, 6))
        ttk.Button(search, text="Найти", command=self.refresh_list).grid(row=0, column=1, padx=(0, 6))
        ttk.Button(search, text="Содержит", command=self.apply_contains).grid(row=0, column=2)
        list_frame = ttk.Frame(self.window)
        list_frame.grid(row=3, column=0, sticky="nsew", padx=12)
        list_frame.columnconfigure(0, weight=1)
        list_frame.rowconfigure(0, weight=1)
        self.listbox = tk.Listbox(list_frame, selectmode="extended", exportselection=False)
        y_scroll = ttk.Scrollbar(list_frame, orient="vertical", command=self.listbox.yview)
        self.listbox.configure(yscrollcommand=y_scroll.set)
        self.listbox.grid(row=0, column=0, sticky="nsew")
        y_scroll.grid(row=0, column=1, sticky="ns")
        self.info_var = tk.StringVar()
        ttk.Label(self.window, textvariable=self.info_var).grid(row=4, column=0, sticky="w", padx=12, pady=(6, 0))
        bottom = ttk.Frame(self.window)
        bottom.grid(row=5, column=0, sticky="ew", padx=12, pady=12)
        bottom.columnconfigure(4, weight=1)
        ttk.Button(bottom, text="Выбрать все", command=lambda: self.listbox.selection_set(0, tk.END)).grid(row=0, column=0, padx=(0, 6))
        ttk.Button(bottom, text="Снять", command=lambda: self.listbox.selection_clear(0, tk.END)).grid(row=0, column=1, padx=(0, 6))
        ttk.Button(bottom, text="Применить", command=self.apply_selected).grid(row=0, column=5, padx=(6, 0))
        ttk.Button(bottom, text="Отмена", command=self.window.destroy).grid(row=0, column=6)
        self._refresh_after_id: str | None = None
        self.search_entry.bind("<KeyRelease>", lambda _e: self.refresh_later())
        self.window.bind("<Return>", lambda _e: self.apply_contains())
        self.window.bind("<Escape>", lambda _e: self.window.destroy())
        self.refresh_list()
        self.search_entry.focus_set()
        self.app._bind_context_menus_recursive(self.window)
        self.app._center_window(self.window)

    def refresh_later(self) -> None:
        if self._refresh_after_id:
            self.window.after_cancel(self._refresh_after_id)
        self._refresh_after_id = self.window.after(250, self.refresh_list)

    def refresh_list(self) -> None:
        q = self.search_var.get().strip().lower()
        matched = [v for v in self.values if q in v.lower()] if q else self.values
        self.visible_values = matched[: self.MAX_VALUES]
        self.listbox.delete(0, tk.END)
        for v in self.visible_values:
            self.listbox.insert(tk.END, v if v else "(пусто)")
        active = self.table.filters.get(self.column)
        if active is not None:
            for i, v in enumerate(self.visible_values):
                if v in active:
                    self.listbox.selection_set(i)
        suffix = "" if len(matched) <= self.MAX_VALUES else f"; показаны первые {self.MAX_VALUES}"
        self.info_var.set(f"Найдено значений: {len(matched)}{suffix}. Enter — фильтр по тексту.")

    def apply_selected(self) -> None:
        selected = {self.visible_values[i] for i in self.listbox.curselection()}
        self.table.contains_filters.pop(self.column, None)
        if selected:
            self.table.filters[self.column] = selected
        else:
            self.table.filters.pop(self.column, None)
        self.table.page = 0
        self.app.apply_view(self.table.key)
        self.window.destroy()

    def apply_contains(self) -> None:
        q = self.search_var.get().strip().lower()
        self.table.filters.pop(self.column, None)
        if q:
            self.table.contains_filters[self.column] = q
        else:
            self.table.contains_filters.pop(self.column, None)
        self.table.page = 0
        self.app.apply_view(self.table.key)
        self.window.destroy()

    def clear_filter(self) -> None:
        self.table.filters.pop(self.column, None)
        self.table.contains_filters.pop(self.column, None)
        self.table.page = 0
        self.app.apply_view(self.table.key)
        self.window.destroy()

    def sort_and_close(self, reverse: bool) -> None:
        self.table.sort_column = self.column
        self.table.sort_reverse = reverse
        self.table.page = 0
        self.app.apply_view(self.table.key)
        self.window.destroy()

    def hide_column(self) -> None:
        self.app.hide_column(self.table.key, self.column)
        self.window.destroy()


