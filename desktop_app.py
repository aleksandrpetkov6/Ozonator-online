from __future__ import annotations

import json
import os
import queue
import subprocess
import sys
import tempfile
import threading
import time as timer
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import tkinter as tk
from tkinter import filedialog, messagebox, ttk

from ozon_constants import *
from ozon_client import OzonClient
from ozon_export import export_worker_cli
from ozon_normalizers import (
    add_related_postings,
    build_offer_map_from_rows,
    normalize_finance_operations,
    normalize_postings,
)
from ozon_utils import (
    date_chunks,
    date_to_ozon_datetime,
    load_rows_cache,
    parse_date,
    save_rows_cache,
    sort_key,
)
from ozon_widgets import DatePicker, FilterDialog, TableState

class DesktopApp:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title(APP_TITLE)
        self.root.geometry("1280x760")
        self.root.minsize(1050, 620)
        self.worker_queue: queue.Queue[tuple[str, Any]] = queue.Queue()
        self.client_id_var = tk.StringVar()
        self.api_key_var = tk.StringVar()
        self.base_url_var = tk.StringVar(value=DEFAULT_BASE_URL)
        self.start_date_var = tk.StringVar(value=(date.today() - timedelta(days=7)).isoformat())
        self.end_date_var = tk.StringVar(value=date.today().isoformat())
        self.save_settings_var = tk.BooleanVar(value=False)
        self.status_text_var = tk.StringVar(value="Готово.")
        self.tables = {key: TableState(key, cols) for key, cols in ALL_COLUMNS_BY_TAB.items()}
        self.active_key = "orders"
        self.tree_to_key: dict[str, str] = {}
        self._last_tree_cell: tuple[str, str, int] | None = None
        self._drag_column: str | None = None
        self._drag_table_key: str | None = None
        self._drag_started = False
        self._drag_start_x = 0
        self._header_click_after_id: str | None = None
        self._export_in_progress = False
        self._load_in_progress = False
        self._load_settings()
        self._build_ui()
        self._configure_shortcuts_and_mouse()
        self._restore_cached_rows()
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self._poll_worker_queue()

    @property
    def active_table(self) -> TableState:
        return self.tables[self.active_key]

    @property
    def active_tree(self) -> ttk.Treeview:
        tree = self.active_table.tree
        assert tree is not None
        return tree

    def _build_ui(self) -> None:
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(1, weight=1)
        toolbar = ttk.Frame(self.root, padding=(8, 4, 8, 3))
        toolbar.grid(row=0, column=0, sticky="ew")
        toolbar.columnconfigure(9, weight=1)
        ttk.Button(toolbar, text="⚙", width=3, command=self.open_settings_window).grid(row=0, column=0, padx=(0, 6))
        self.start_entry = ttk.Entry(toolbar, textvariable=self.start_date_var, width=12)
        self.start_entry.grid(row=0, column=1, padx=(0, 2))
        ttk.Button(toolbar, text="▾", width=2, command=lambda: DatePicker(self, self.start_date_var)).grid(row=0, column=2, padx=(0, 3))
        ttk.Label(toolbar, text="—").grid(row=0, column=3, padx=(0, 3))
        self.end_entry = ttk.Entry(toolbar, textvariable=self.end_date_var, width=12)
        self.end_entry.grid(row=0, column=4, padx=(0, 2))
        ttk.Button(toolbar, text="▾", width=2, command=lambda: DatePicker(self, self.end_date_var)).grid(row=0, column=5, padx=(0, 6))
        self.load_button = ttk.Button(toolbar, text="↻", width=3, command=self.load_active_tab)
        self.load_button.grid(row=0, column=6, padx=(0, 4))
        self.export_button = ttk.Button(toolbar, text="⇩", width=3, command=self.export_xlsx, state="disabled")
        self.export_button.grid(row=0, column=7, padx=(0, 8))
        ttk.Label(toolbar, textvariable=self.status_text_var, anchor="e").grid(row=0, column=9, sticky="ew", padx=(8, 8))
        self.columns_button = ttk.Button(toolbar, text="Столбцы", command=self.open_columns_window)
        self.columns_button.grid(row=0, column=10, sticky="e")

        self.notebook = ttk.Notebook(self.root)
        self.notebook.grid(row=1, column=0, sticky="nsew")
        self.notebook.bind("<<NotebookTabChanged>>", self._on_tab_changed)
        for key, table in self.tables.items():
            frame = ttk.Frame(self.notebook, padding=(8, 0, 8, 0))
            frame.columnconfigure(0, weight=1)
            frame.rowconfigure(0, weight=1)
            self.notebook.add(frame, text=TAB_TITLES[key])
            tree = ttk.Treeview(frame, columns=table.columns, displaycolumns=table.visible_columns, show="headings", selectmode="extended")
            table.tree = tree
            self.tree_to_key[str(tree)] = key
            for col in table.columns:
                tree.heading(col, text=self._heading_text(table, col), anchor="w")
                tree.column(col, width=self._column_width(table, col), minwidth=50, anchor="w", stretch=False)
            y_scroll = ttk.Scrollbar(frame, orient="vertical", command=tree.yview)
            x_scroll = ttk.Scrollbar(frame, orient="horizontal", command=tree.xview)
            tree.configure(yscrollcommand=y_scroll.set, xscrollcommand=x_scroll.set)
            tree.grid(row=0, column=0, sticky="nsew")
            y_scroll.grid(row=0, column=1, sticky="ns")
            x_scroll.grid(row=1, column=0, sticky="ew")

    def _on_tab_changed(self, _event: tk.Event | None = None) -> None:
        idx = self.notebook.index(self.notebook.select())
        self.active_key = list(self.tables.keys())[idx]
        self._sync_status_for_active_tab()
        self._sync_buttons()

    def _heading_text(self, table: TableState, column: str) -> str:
        mark = "●" if column in table.filters or column in table.contains_filters else "▾"
        sort_mark = " ↑" if table.sort_column == column and not table.sort_reverse else " ↓" if table.sort_column == column else ""
        return f"{column} {mark}{sort_mark}"

    def _refresh_headings(self, table_key: str | None = None) -> None:
        keys = [table_key] if table_key else list(self.tables.keys())
        for key in keys:
            table = self.tables[key]
            tree = table.tree
            if not tree:
                continue
            for col in table.columns:
                tree.heading(col, text=self._heading_text(table, col), anchor="w")

    def _column_width(self, table: TableState, column: str) -> int:
        if column in table.column_widths:
            return max(50, int(table.column_widths[column]))
        if column in {"Название товара", "Детали перевозчика по статусу", "Список услуг"}:
            return 240
        if column in {"Номер отправления", "Номер заказа", "Принят в обработку", "Дата отгрузки", "Дата операции"}:
            return 150
        if column in {"Схема", "Количество", "Статус", "Подстатус", "Валюта"}:
            return 95
        return 130

    def _center_window(self, window: tk.Toplevel) -> None:
        window.update_idletasks()
        x = self.root.winfo_x() + max(0, (self.root.winfo_width() - window.winfo_width()) // 2)
        y = self.root.winfo_y() + max(0, (self.root.winfo_height() - window.winfo_height()) // 2)
        window.geometry(f"+{x}+{y}")

    def open_settings_window(self) -> None:
        win = tk.Toplevel(self.root)
        win.title("Настройки Ozon")
        win.transient(self.root)
        win.grab_set()
        win.resizable(False, False)
        frame = ttk.Frame(win, padding=16)
        frame.grid(row=0, column=0, sticky="nsew")
        frame.columnconfigure(1, weight=1)
        ttk.Label(frame, text="Настройки доступа к Ozon", font=("Segoe UI", 12, "bold")).grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 12))
        ttk.Label(frame, text="Client-Id / ID кабинета").grid(row=1, column=0, sticky="w", pady=(0, 6))
        e1 = ttk.Entry(frame, textvariable=self.client_id_var, width=42)
        e1.grid(row=1, column=1, sticky="ew", pady=(0, 6), padx=(12, 0))
        ttk.Label(frame, text="Api-Key / ключ API").grid(row=2, column=0, sticky="w", pady=(0, 6))
        ttk.Entry(frame, textvariable=self.api_key_var, show="*", width=42).grid(row=2, column=1, sticky="ew", pady=(0, 6), padx=(12, 0))
        ttk.Label(frame, text="Base URL").grid(row=3, column=0, sticky="w", pady=(0, 6))
        ttk.Entry(frame, textvariable=self.base_url_var, width=42).grid(row=3, column=1, sticky="ew", pady=(0, 6), padx=(12, 0))
        ttk.Checkbutton(frame, text="Сохранить ключи локально", variable=self.save_settings_var).grid(row=4, column=0, columnspan=2, sticky="w", pady=(8, 14))
        buttons = ttk.Frame(frame)
        buttons.grid(row=5, column=0, columnspan=2, sticky="e")
        ttk.Button(buttons, text="Сохранить", command=lambda: self._save_settings_and_close(win)).grid(row=0, column=0, padx=(0, 8))
        ttk.Button(buttons, text="Закрыть", command=win.destroy).grid(row=0, column=1)
        self._bind_context_menus_recursive(win)
        win.bind("<Escape>", lambda _e: win.destroy())
        win.bind("<Return>", lambda _e: self._save_settings_and_close(win))
        e1.focus_set()
        self._center_window(win)

    def _save_settings_and_close(self, win: tk.Toplevel) -> None:
        try:
            self._save_settings()
        except Exception as exc:
            messagebox.showerror("Ошибка сохранения", str(exc), parent=win)
            return
        win.destroy()
        self.status_text_var.set("Настройки сохранены." if self.save_settings_var.get() else "Настройки применены.")

    def open_columns_window(self) -> None:
        table = self.active_table
        win = tk.Toplevel(self.root)
        win.title(f"Столбцы — {TAB_TITLES[table.key]}")
        win.transient(self.root)
        win.grab_set()
        win.geometry("380x540")
        win.columnconfigure(0, weight=1)
        win.rowconfigure(1, weight=1)
        ttk.Label(win, text="Показывать столбцы", font=("Segoe UI", 11, "bold")).grid(row=0, column=0, sticky="w", padx=12, pady=(12, 8))
        frame = ttk.Frame(win)
        frame.grid(row=1, column=0, sticky="nsew", padx=12)
        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(0, weight=1)
        canvas = tk.Canvas(frame, highlightthickness=0)
        scroll = ttk.Scrollbar(frame, orient="vertical", command=canvas.yview)
        inner = ttk.Frame(canvas)
        inner.bind("<Configure>", lambda _e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.create_window((0, 0), window=inner, anchor="nw")
        canvas.configure(yscrollcommand=scroll.set)
        canvas.grid(row=0, column=0, sticky="nsew")
        scroll.grid(row=0, column=1, sticky="ns")
        vars_by_col = {col: tk.BooleanVar(value=col in table.visible_columns) for col in table.columns}
        for col in table.columns:
            ttk.Checkbutton(inner, text=col, variable=vars_by_col[col]).pack(anchor="w", fill="x", pady=1)
        bottom = ttk.Frame(win)
        bottom.grid(row=2, column=0, sticky="ew", padx=12, pady=12)
        bottom.columnconfigure(1, weight=1)
        ttk.Button(bottom, text="Все", command=lambda: [v.set(True) for v in vars_by_col.values()]).grid(row=0, column=0, padx=(0, 6))
        def apply() -> None:
            selected = [col for col in table.columns if vars_by_col[col].get()]
            if not selected:
                messagebox.showerror("Столбцы", "Должен быть выбран хотя бы один столбец.", parent=win)
                return
            current = [col for col in table.visible_columns if col in selected]
            rest = [col for col in table.columns if col in selected and col not in current]
            table.visible_columns = current + rest
            assert table.tree is not None
            table.tree.configure(displaycolumns=table.visible_columns)
            self._refresh_headings(table.key)
            self._save_settings_silent()
            win.destroy()
        ttk.Button(bottom, text="Применить", command=apply).grid(row=0, column=2, padx=(6, 0))
        ttk.Button(bottom, text="Отмена", command=win.destroy).grid(row=0, column=3)
        self._bind_context_menus_recursive(win)
        self._center_window(win)

    def _create_context_menus(self) -> None:
        self.entry_menu = tk.Menu(self.root, tearoff=0)
        self.entry_menu.add_command(label="Вырезать", command=lambda: self._cut_widget(self.root.focus_get()))
        self.entry_menu.add_command(label="Копировать", command=lambda: self._copy_widget(self.root.focus_get()))
        self.entry_menu.add_command(label="Вставить", command=lambda: self._paste_widget(self.root.focus_get()))
        self.entry_menu.add_separator()
        self.entry_menu.add_command(label="Выделить всё", command=lambda: self._select_all_widget(self.root.focus_get()))
        self.tree_menu = tk.Menu(self.root, tearoff=0)
        self.tree_menu.add_command(label="Копировать ячейку", command=self.copy_current_cell)
        self.tree_menu.add_command(label="Копировать выбранные строки", command=self.copy_selected_rows)
        self.tree_menu.add_command(label="Копировать всю вкладку", command=self.copy_all_rows)
        self.tree_menu.add_separator()
        self.tree_menu.add_command(label="Выделить все строки на странице", command=self.select_all_rows)
        self.tree_menu.add_command(label="Столбцы...", command=self.open_columns_window)
        self.tree_menu.add_command(label="Сбросить фильтры", command=self.clear_all_filters)
        self.tree_menu.add_separator()
        self.tree_menu.add_command(label="Следующая страница", command=self.next_page)
        self.tree_menu.add_command(label="Предыдущая страница", command=self.prev_page)

    def _configure_shortcuts_and_mouse(self) -> None:
        self._create_context_menus()
        self._bind_context_menus_recursive(self.root)
        for seq, handler in {
            "<Control-a>": self._hotkey_select_all, "<Control-A>": self._hotkey_select_all,
            "<Control-c>": self._hotkey_copy, "<Control-C>": self._hotkey_copy,
            "<Control-v>": self._hotkey_paste, "<Control-V>": self._hotkey_paste,
            "<Control-x>": self._hotkey_cut, "<Control-X>": self._hotkey_cut,
            "<Control-e>": self._hotkey_export, "<Control-E>": self._hotkey_export,
            "<Control-s>": self._hotkey_export, "<Control-S>": self._hotkey_export,
            "<F5>": self._hotkey_load,
            "<Prior>": lambda _e: self.prev_page(),
            "<Next>": lambda _e: self.next_page(),
        }.items():
            self.root.bind_all(seq, handler, add="+")
        for table in self.tables.values():
            tree = table.tree
            if tree is None:
                continue
            tree.bind("<Button-3>", self._show_tree_menu, add="+")
            tree.bind("<Double-1>", self._tree_double_click, add="+")
            tree.bind("<ButtonPress-1>", self._tree_button_press, add="+")
            tree.bind("<B1-Motion>", self._tree_drag_motion, add="+")
            tree.bind("<ButtonRelease-1>", self._tree_button_release, add="+")
            tree.bind("<MouseWheel>", self._tree_mousewheel, add="+")
            tree.bind("<Shift-MouseWheel>", self._tree_shift_mousewheel, add="+")

    def _bind_context_menus_recursive(self, widget: tk.Widget) -> None:
        if isinstance(widget, (tk.Entry, ttk.Entry, tk.Text, ttk.Combobox)):
            widget.bind("<Button-3>", self._show_entry_menu, add="+")
            widget.bind("<Control-KeyPress>", self._text_ctrl_keypress, add="+")
        for child in widget.winfo_children():
            self._bind_context_menus_recursive(child)

    def _text_ctrl_keypress(self, event: tk.Event) -> str | None:
        key = str(getattr(event, "keysym", "")).lower()
        char = str(getattr(event, "char", "")).lower()
        widget = event.widget
        if key in {"a", "ф"} or char in {"a", "ф"}:
            self._select_all_widget(widget); return "break"
        if key in {"c", "с"} or char in {"c", "с"}:
            self._copy_widget(widget); return "break"
        if key in {"v", "м"} or char in {"v", "м"}:
            self._paste_widget(widget); return "break"
        if key in {"x", "ч"} or char in {"x", "ч"}:
            self._cut_widget(widget); return "break"
        return None

    def _show_entry_menu(self, event: tk.Event) -> str:
        event.widget.focus_set()
        try:
            self.entry_menu.tk_popup(event.x_root, event.y_root)
        finally:
            self.entry_menu.grab_release()
        return "break"

    def _table_from_tree(self, tree: ttk.Treeview) -> TableState:
        return self.tables[self.tree_to_key.get(str(tree), self.active_key)]

    def _show_tree_menu(self, event: tk.Event) -> str:
        tree = event.widget
        table = self._table_from_tree(tree)
        self.active_key = table.key
        row_id = tree.identify_row(event.y)
        col_id = tree.identify_column(event.x)
        if row_id:
            tree.selection_set(row_id)
            if col_id:
                self._last_tree_cell = (table.key, row_id, max(0, int(col_id.replace("#", "")) - 1))
        try:
            self.tree_menu.tk_popup(event.x_root, event.y_root)
        finally:
            self.tree_menu.grab_release()
        return "break"

    def _tree_button_press(self, event: tk.Event) -> None:
        tree = event.widget
        table = self._table_from_tree(tree)
        self.active_key = table.key
        if tree.identify_region(event.x, event.y) != "heading":
            return
        try:
            idx = int(tree.identify_column(event.x).replace("#", "")) - 1
            self._drag_column = table.visible_columns[idx]
            self._drag_table_key = table.key
            self._drag_start_x = event.x
            self._drag_started = False
        except Exception:
            self._drag_column = None
            self._drag_table_key = None

    def _tree_drag_motion(self, event: tk.Event) -> None:
        if self._drag_column and abs(event.x - self._drag_start_x) > 12:
            self._drag_started = True

    def _tree_button_release(self, event: tk.Event) -> str | None:
        if not self._drag_column or not self._drag_table_key:
            return None
        tree = event.widget
        table = self.tables[self._drag_table_key]
        column = self._drag_column
        self._drag_column = None
        self._drag_table_key = None
        region = tree.identify_region(event.x, event.y)
        if region == "separator":
            self._remember_column_widths(table)
            self._save_settings_silent()
            return None
        if self._drag_started and region == "heading":
            try:
                target = int(tree.identify_column(event.x).replace("#", "")) - 1
                self.move_column(table.key, column, target)
            except Exception:
                pass
            return "break"
        if region == "heading" and not self._drag_started:
            if self._header_click_after_id:
                self.root.after_cancel(self._header_click_after_id)
            self._header_click_after_id = self.root.after(220, lambda t=table, c=column: FilterDialog(self, t, c))
            return "break"
        return None

    def _tree_double_click(self, event: tk.Event) -> str:
        tree = event.widget
        table = self._table_from_tree(tree)
        self.active_key = table.key
        if tree.identify_region(event.x, event.y) == "heading":
            if self._header_click_after_id:
                self.root.after_cancel(self._header_click_after_id)
                self._header_click_after_id = None
            try:
                idx = int(tree.identify_column(event.x).replace("#", "")) - 1
                self.sort_by_column(table.key, table.visible_columns[idx])
            except Exception:
                pass
            return "break"
        row_id = tree.identify_row(event.y)
        col_id = tree.identify_column(event.x)
        if row_id and col_id:
            self._last_tree_cell = (table.key, row_id, max(0, int(col_id.replace("#", "")) - 1))
            self.copy_current_cell()
        return "break"

    def _remember_column_widths(self, table: TableState) -> None:
        if not table.tree:
            return
        for col in table.columns:
            try:
                table.column_widths[col] = int(table.tree.column(col, "width"))
            except Exception:
                pass

    def sort_by_column(self, table_key: str, column: str) -> None:
        table = self.tables[table_key]
        if table.sort_column == column:
            table.sort_reverse = not table.sort_reverse
        else:
            table.sort_column = column
            table.sort_reverse = False
        table.page = 0
        self.apply_view(table.key)
        self._save_settings_silent()

    def move_column(self, table_key: str, column: str, target_index: int) -> None:
        table = self.tables[table_key]
        if column not in table.visible_columns:
            return
        table.visible_columns.remove(column)
        table.visible_columns.insert(max(0, min(target_index, len(table.visible_columns))), column)
        assert table.tree is not None
        table.tree.configure(displaycolumns=table.visible_columns)
        self._refresh_headings(table.key)
        self._save_settings_silent()

    def hide_column(self, table_key: str, column: str) -> None:
        table = self.tables[table_key]
        if column in table.visible_columns and len(table.visible_columns) > 1:
            table.visible_columns.remove(column)
            assert table.tree is not None
            table.tree.configure(displaycolumns=table.visible_columns)
            self._refresh_headings(table.key)
            self._save_settings_silent()

    def clear_all_filters(self) -> None:
        table = self.active_table
        table.filters.clear()
        table.contains_filters.clear()
        table.page = 0
        self.apply_view(table.key)

    def _tree_mousewheel(self, event: tk.Event) -> str:
        event.widget.yview_scroll((-1 if event.delta > 0 else 1) * 3, "units")
        return "break"

    def _tree_shift_mousewheel(self, event: tk.Event) -> str:
        event.widget.xview_scroll((-1 if event.delta > 0 else 1) * 3, "units")
        return "break"

    def _focused_widget(self) -> tk.Widget | None:
        return self.root.focus_get()

    def _is_text_input(self, widget: tk.Widget | None) -> bool:
        return isinstance(widget, (tk.Entry, ttk.Entry, tk.Text, ttk.Combobox))

    def _hotkey_select_all(self, _event: tk.Event) -> str | None:
        widget = self._focused_widget()
        if widget in [t.tree for t in self.tables.values()]:
            self.select_all_rows(); return "break"
        if self._is_text_input(widget):
            self._select_all_widget(widget); return "break"
        return None

    def _hotkey_copy(self, _event: tk.Event) -> str | None:
        widget = self._focused_widget()
        if widget in [t.tree for t in self.tables.values()]:
            self.copy_selected_rows(); return "break"
        if self._is_text_input(widget):
            self._copy_widget(widget); return "break"
        return None

    def _hotkey_paste(self, _event: tk.Event) -> str | None:
        widget = self._focused_widget()
        if self._is_text_input(widget):
            self._paste_widget(widget); return "break"
        return None

    def _hotkey_cut(self, _event: tk.Event) -> str | None:
        widget = self._focused_widget()
        if self._is_text_input(widget):
            self._cut_widget(widget); return "break"
        return None

    def _hotkey_export(self, _event: tk.Event) -> str:
        self.export_xlsx(); return "break"

    def _hotkey_load(self, _event: tk.Event) -> str:
        self.load_active_tab(); return "break"

    def _select_all_widget(self, widget: tk.Widget | None) -> None:
        try:
            if isinstance(widget, tk.Text):
                widget.tag_add("sel", "1.0", "end")
            elif isinstance(widget, (tk.Entry, ttk.Entry, ttk.Combobox)):
                widget.select_range(0, tk.END)
                widget.icursor(tk.END)
        except Exception:
            pass

    def _copy_widget(self, widget: tk.Widget | None) -> None:
        try:
            text = widget.selection_get() if widget else ""
            self.root.clipboard_clear(); self.root.clipboard_append(text)
        except Exception:
            pass

    def _paste_widget(self, widget: tk.Widget | None) -> None:
        try:
            text = self.root.clipboard_get()
            if isinstance(widget, tk.Text):
                widget.insert(tk.INSERT, text)
            elif isinstance(widget, (tk.Entry, ttk.Entry, ttk.Combobox)):
                try:
                    widget.delete("sel.first", "sel.last")
                except Exception:
                    pass
                widget.insert(tk.INSERT, text)
        except Exception:
            pass

    def _cut_widget(self, widget: tk.Widget | None) -> None:
        try:
            self._copy_widget(widget)
            if isinstance(widget, tk.Text):
                widget.delete("sel.first", "sel.last")
            elif isinstance(widget, (tk.Entry, ttk.Entry, ttk.Combobox)):
                widget.delete("sel.first", "sel.last")
        except Exception:
            pass

    def select_all_rows(self) -> None:
        tree = self.active_tree
        tree.selection_set(tree.get_children())
        tree.focus_set()

    def _current_display_rows(self, table: TableState) -> list[dict[str, Any]]:
        start = table.page * table.page_size
        return table.filtered_rows[start:start + table.page_size]

    def _row_from_iid(self, table: TableState, iid: str) -> dict[str, Any] | None:
        try:
            idx = int(iid)
        except Exception:
            return None
        if 0 <= idx < len(table.filtered_rows):
            return table.filtered_rows[idx]
        return None

    def copy_current_cell(self) -> None:
        if not self._last_tree_cell:
            return
        table_key, row_id, idx = self._last_tree_cell
        table = self.tables[table_key]
        row = self._row_from_iid(table, row_id)
        if row is None or idx >= len(table.visible_columns):
            return
        value = str(row.get(table.visible_columns[idx], ""))
        self.root.clipboard_clear(); self.root.clipboard_append(value)

    def copy_selected_rows(self) -> None:
        table = self.active_table
        tree = self.active_tree
        rows = []
        for iid in tree.selection():
            row = self._row_from_iid(table, iid)
            if row is not None:
                rows.append(row)
        if not rows:
            return
        text = "\t".join(table.visible_columns) + "\n" + "\n".join("\t".join(str(r.get(c, "")) for c in table.visible_columns) for r in rows)
        self.root.clipboard_clear(); self.root.clipboard_append(text)

    def copy_all_rows(self) -> None:
        table = self.active_table
        rows = table.filtered_rows
        if not rows:
            return
        text = "\t".join(table.visible_columns) + "\n" + "\n".join("\t".join(str(r.get(c, "")) for c in table.visible_columns) for r in rows[:10000])
        if len(rows) > 10000:
            text += f"\n... скопированы первые 10000 строк из {len(rows)}"
        self.root.clipboard_clear(); self.root.clipboard_append(text)

    def prev_page(self) -> str:
        table = self.active_table
        if table.page > 0:
            table.page -= 1
            self._fill_table(table.key)
        return "break"

    def next_page(self) -> str:
        table = self.active_table
        max_page = max(0, (len(table.filtered_rows) - 1) // table.page_size)
        if table.page < max_page:
            table.page += 1
            self._fill_table(table.key)
        return "break"

    def _load_settings(self) -> None:
        if not SETTINGS_PATH.exists():
            return
        try:
            data = json.loads(SETTINGS_PATH.read_text(encoding="utf-8"))
        except Exception:
            return
        self.client_id_var.set(data.get("client_id", ""))
        self.api_key_var.set(data.get("api_key", ""))
        self.base_url_var.set(data.get("base_url", DEFAULT_BASE_URL))
        self.start_date_var.set(data.get("start_date", self.start_date_var.get()))
        self.end_date_var.set(data.get("end_date", self.end_date_var.get()))
        self.save_settings_var.set(bool(data.get("save_credentials", False)))
        table_state = data.get("tables", {})
        if isinstance(table_state, dict):
            for key, table in self.tables.items():
                stored = table_state.get(key, {})
                if not isinstance(stored, dict):
                    continue
                visible = stored.get("visible_columns")
                if isinstance(visible, list):
                    selected = [c for c in visible if c in table.columns]
                    if selected:
                        table.visible_columns = selected
                widths = stored.get("column_widths")
                if isinstance(widths, dict):
                    table.column_widths = {str(k): int(v) for k, v in widths.items() if str(k) in table.columns and str(v).isdigit()}

    def _save_settings(self) -> None:
        APP_DIR.mkdir(parents=True, exist_ok=True)
        for table in self.tables.values():
            self._remember_column_widths(table)
        data: dict[str, Any] = {
            "base_url": self.base_url_var.get().strip() or DEFAULT_BASE_URL,
            "start_date": self.start_date_var.get().strip(),
            "end_date": self.end_date_var.get().strip(),
            "save_credentials": bool(self.save_settings_var.get()),
            "tables": {
                key: {"visible_columns": t.visible_columns, "column_widths": t.column_widths}
                for key, t in self.tables.items()
            },
        }
        if self.save_settings_var.get():
            data["client_id"] = self.client_id_var.get().strip()
            data["api_key"] = self.api_key_var.get().strip()
        SETTINGS_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    def _save_settings_silent(self) -> None:
        try:
            self._save_settings()
        except Exception:
            pass

    def _restore_cached_rows(self) -> None:
        for key, table in self.tables.items():
            table.rows = load_rows_cache(key)
            if table.rows:
                self.apply_view(key, update_status=False)
        self._sync_status_for_active_tab()
        self._sync_buttons()

    def _sync_buttons(self) -> None:
        table = self.active_table
        self.export_button.configure(state="normal" if table.filtered_rows and not self._export_in_progress else "disabled")
        self.load_button.configure(state="disabled" if self._load_in_progress else "normal")

    def _sync_status_for_active_tab(self) -> None:
        table = self.active_table
        if table.rows:
            self.status_text_var.set(self._status_summary(table, prefix="Готово."))
        else:
            self.status_text_var.set(f"{TAB_TITLES[table.key]}: данных нет.")

    def _status_summary(self, table: TableState, prefix: str = "Готово.", elapsed: float | None = None) -> str:
        if table.key == "orders":
            orders = len({r.get("Номер заказа") for r in table.rows if r.get("Номер заказа")})
            postings = len({r.get("Номер отправления") for r in table.rows if r.get("Номер отправления")})
            base = f"{prefix} Заказов: {orders}; отправлений: {postings}; строк: {len(table.rows)}"
        else:
            postings = len({r.get("Номер отправления") for r in table.rows if r.get("Номер отправления")})
            base = f"{prefix} Операций/строк: {len(table.rows)}; отправлений: {postings}"
        if table.filtered_rows and len(table.filtered_rows) != len(table.rows):
            base += f"; по фильтру: {len(table.filtered_rows)}"
        if table.filtered_rows:
            start = table.page * table.page_size + 1
            end = min((table.page + 1) * table.page_size, len(table.filtered_rows))
            base += f"; показано: {start}-{end}"
        if elapsed is not None:
            base += f"; время: {elapsed:.1f} сек."
        return base

    def _on_close(self) -> None:
        try:
            self._save_settings()
        except Exception:
            pass
        self.root.destroy()

    def load_active_tab(self) -> None:
        if self._load_in_progress:
            return
        try:
            start_date = parse_date(self.start_date_var.get())
            end_date = parse_date(self.end_date_var.get())
        except ValueError as exc:
            messagebox.showerror("Ошибка даты", str(exc)); return
        self.start_date_var.set(start_date.isoformat())
        self.end_date_var.set(end_date.isoformat())
        if end_date < start_date:
            messagebox.showerror("Ошибка даты", "Дата по не может быть раньше даты с"); return
        client_id = self.client_id_var.get().strip()
        api_key = self.api_key_var.get().strip()
        base_url = self.base_url_var.get().strip() or DEFAULT_BASE_URL
        if not client_id or not api_key:
            messagebox.showerror("Нет ключей Ozon", "Нажмите кнопку ⚙ и заполните Client-Id и Api-Key.")
            self.open_settings_window(); return
        try:
            self._save_settings()
        except Exception:
            pass
        self._load_in_progress = True
        self._sync_buttons()
        table = self.active_table
        self.status_text_var.set("Загрузка по периоду принятия в обработку. Осталось: рассчитываю...")
        self._clear_table(table.key)
        table.filters.clear(); table.contains_filters.clear(); table.sort_column = None; table.sort_reverse = False; table.page = 0
        self._refresh_headings(table.key)
        args = (table.key, client_id, api_key, base_url, start_date, end_date)
        threading.Thread(target=self._load_worker, args=args, daemon=True).start()

    def _load_worker(self, tab_key: str, client_id: str, api_key: str, base_url: str, start_date: date, end_date: date) -> None:
        started = timer.perf_counter()
        try:
            client = OzonClient(client_id, api_key, base_url)
            if tab_key == "orders":
                chunks = date_chunks(start_date, end_date, MAX_REQUEST_DAYS)
                schemas = ["FBS", "FBO"]
                total = len(chunks) * len(schemas)
                step = 0
                all_rows: list[dict[str, Any]] = []
                for schema in schemas:
                    for chunk_start, chunk_end in chunks:
                        step += 1
                        self._put_eta(started, step, total)
                        postings = client.list_postings(schema, date_to_ozon_datetime(chunk_start), date_to_ozon_datetime(chunk_end, end_of_day=True))
                        shipment_dates = {}
                        if schema == "FBO":
                            shipment_dates = client.shipment_dates_from_postings_report("fbo", chunk_start, chunk_end)
                        all_rows.extend(normalize_postings(postings, schema, shipment_dates))
                all_rows = add_related_postings(all_rows)
            else:
                chunks = date_chunks(start_date, end_date, MAX_FINANCE_DAYS)
                total = len(chunks)
                operations: list[dict[str, Any]] = []
                for step, (chunk_start, chunk_end) in enumerate(chunks, start=1):
                    self._put_eta(started, step, total)
                    operations.extend(client.list_finance_transactions(date_to_ozon_datetime(chunk_start), date_to_ozon_datetime(chunk_end, end_of_day=True)))
                sku_offer_map = build_offer_map_from_rows(load_rows_cache("orders"))
                finance_skus = []
                for operation in operations:
                    for item in operation.get("items") or []:
                        if isinstance(item, dict):
                            value = str(item.get("sku") or item.get("product_id") or item.get("item_id") or "").strip()
                            if value and value not in sku_offer_map:
                                finance_skus.append(value)
                if finance_skus:
                    sku_offer_map.update(client.product_offer_map_by_skus(finance_skus))
                all_rows = normalize_finance_operations(operations, sku_offer_map)
            save_rows_cache(tab_key, all_rows)
            self.worker_queue.put(("success", {"tab": tab_key, "rows": all_rows, "elapsed": timer.perf_counter() - started}))
        except Exception as exc:
            self.worker_queue.put(("error", {"tab": tab_key, "message": str(exc)}))

    def _put_eta(self, started: float, step: int, total: int) -> None:
        if step <= 1:
            self.worker_queue.put(("status", "Загрузка. Осталось: рассчитываю..."))
            return
        elapsed = timer.perf_counter() - started
        avg = elapsed / max(1, step - 1)
        remaining = max(0, int(avg * (total - step + 1)))
        self.worker_queue.put(("status", f"Загрузка. Осталось примерно: {remaining} сек."))

    def _poll_worker_queue(self) -> None:
        try:
            while True:
                event, payload = self.worker_queue.get_nowait()
                if event == "status":
                    self.status_text_var.set(payload)
                elif event == "success":
                    tab = payload["tab"]
                    table = self.tables[tab]
                    table.rows = payload["rows"]
                    self.apply_view(tab, update_status=False)
                    self._load_in_progress = False
                    self._sync_buttons()
                    if tab == self.active_key:
                        self.status_text_var.set(self._status_summary(table, prefix="Готово.", elapsed=payload["elapsed"]))
                    if not table.rows:
                        messagebox.showinfo("Нет данных", "За выбранный период данные не найдены.")
                elif event == "error":
                    self._load_in_progress = False
                    self._sync_buttons()
                    self.status_text_var.set("Ошибка загрузки")
                    messagebox.showerror("Ошибка", payload["message"])
                elif event == "export_status":
                    self.status_text_var.set(payload)
                elif event == "export_success":
                    self._export_in_progress = False
                    self._sync_buttons()
                    self.status_text_var.set(f"Экспорт готов. Время: {payload['elapsed']:.1f} сек.")
                    messagebox.showinfo("Готово", f"Файл сохранен:\n{payload['path']}")
                elif event == "export_error":
                    self._export_in_progress = False
                    self._sync_buttons()
                    self.status_text_var.set("Ошибка экспорта")
                    messagebox.showerror("Ошибка экспорта", payload)
        except queue.Empty:
            pass
        self.root.after(150, self._poll_worker_queue)

    def _clear_table(self, tab_key: str) -> None:
        table = self.tables[tab_key]
        if table.fill_after_id:
            try:
                self.root.after_cancel(table.fill_after_id)
            except Exception:
                pass
            table.fill_after_id = None
        table.display_generation += 1
        tree = table.tree
        if tree:
            children = tree.get_children()
            if children:
                tree.delete(*children)
        table.rows = []
        table.filtered_rows = []
        table.page = 0

    def apply_view(self, tab_key: str, update_status: bool = True) -> None:
        table = self.tables[tab_key]
        rows = table.rows
        for col, allowed in table.filters.items():
            rows = [r for r in rows if str(r.get(col, "")) in allowed]
        for col, q in table.contains_filters.items():
            rows = [r for r in rows if q in str(r.get(col, "")).lower()]
        if table.sort_column:
            rows = sorted(rows, key=lambda r: sort_key(r.get(table.sort_column, "")), reverse=table.sort_reverse)
        table.filtered_rows = rows
        max_page = max(0, (len(rows) - 1) // table.page_size)
        table.page = min(table.page, max_page)
        self._fill_table(tab_key)
        self._refresh_headings(tab_key)
        if update_status and tab_key == self.active_key:
            self.status_text_var.set(self._status_summary(table))
        self._sync_buttons()

    def _fill_table(self, tab_key: str) -> None:
        table = self.tables[tab_key]
        tree = table.tree
        if not tree:
            return
        if table.fill_after_id:
            try:
                self.root.after_cancel(table.fill_after_id)
            except Exception:
                pass
        table.display_generation += 1
        generation = table.display_generation
        children = tree.get_children()
        if children:
            tree.delete(*children)
        total = len(table.filtered_rows)
        start_offset = table.page * table.page_size
        end_offset = min(start_offset + table.page_size, total)
        rows = table.filtered_rows[start_offset:end_offset]
        batch_size = 500
        def insert_batch(start: int = 0) -> None:
            if generation != table.display_generation:
                return
            end = min(start + batch_size, len(rows))
            for i in range(start, end):
                absolute_i = start_offset + i
                row = rows[i]
                tree.insert("", "end", iid=str(absolute_i), values=[row.get(col, "") for col in table.columns])
            if end < len(rows):
                table.fill_after_id = self.root.after(1, lambda: insert_batch(end))
            else:
                table.fill_after_id = None
                if tab_key == self.active_key:
                    self.status_text_var.set(self._status_summary(table))
        insert_batch()

    def export_xlsx(self) -> None:
        if self._export_in_progress:
            return
        table = self.active_table
        rows = list(table.filtered_rows if table.filtered_rows else table.rows)
        if not rows:
            messagebox.showinfo("Нет данных", "Сначала загрузите данные."); return
        default_prefix = "ozon_orders" if table.key == "orders" else "ozon_economy"
        default = f"{default_prefix}_{self.start_date_var.get().replace('-', '')}_{self.end_date_var.get().replace('-', '')}.xlsx"
        path = filedialog.asksaveasfilename(title="Сохранить XLSX", defaultextension=".xlsx", initialfile=default, filetypes=[("Excel files", "*.xlsx")])
        if not path:
            return
        columns = list(table.visible_columns)
        self._export_in_progress = True
        self._sync_buttons()
        self.status_text_var.set("Экспорт выполняется в фоне...")
        threading.Thread(target=self._export_worker, args=(rows, path, columns, TAB_TITLES[table.key]), daemon=True).start()

    def _export_worker(self, rows: list[dict[str, Any]], path: str, columns: list[str], sheet_name: str) -> None:
        started = timer.perf_counter()
        temp_path = ""
        try:
            APP_DIR.mkdir(parents=True, exist_ok=True)
            fd, temp_path = tempfile.mkstemp(prefix="store_economics_export_", suffix=".jsonl", dir=str(APP_DIR))
            os.close(fd)
            total = len(rows)
            with open(temp_path, "w", encoding="utf-8") as fh:
                for i, row in enumerate(rows, start=1):
                    fh.write(json.dumps(row, ensure_ascii=False) + "\n")
                    if i % 5000 == 0:
                        self.worker_queue.put(("export_status", f"Подготовка экспорта: {i}/{total}"))
                        timer.sleep(0.01)
            self.worker_queue.put(("export_status", "Экспорт выполняется отдельным процессом..."))
            columns_arg = json.dumps(columns, ensure_ascii=False)
            if getattr(sys, "frozen", False):
                cmd = [sys.executable, "--export-worker", temp_path, path, columns_arg, sheet_name]
            else:
                cmd = [sys.executable, str(Path(__file__).resolve()), "--export-worker", temp_path, path, columns_arg, sheet_name]
            completed = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
            if completed.returncode != 0:
                err_path = path + ".error.txt"
                message = completed.stderr.strip()
                if Path(err_path).exists():
                    message = Path(err_path).read_text(encoding="utf-8", errors="replace")
                    try: Path(err_path).unlink()
                    except Exception: pass
                raise RuntimeError(message or "Не удалось выполнить экспорт")
            self.worker_queue.put(("export_success", {"path": path, "elapsed": timer.perf_counter() - started}))
        except Exception as exc:
            self.worker_queue.put(("export_error", str(exc)))
        finally:
            if temp_path:
                try:
                    Path(temp_path).unlink()
                except Exception:
                    pass


def main() -> None:
    if len(sys.argv) > 1 and sys.argv[1] == "--export-worker":
        raise SystemExit(export_worker_cli())
    root = tk.Tk()
    try:
        style = ttk.Style(root)
        if "vista" in style.theme_names():
            style.theme_use("vista")
    except Exception:
        pass
    DesktopApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
