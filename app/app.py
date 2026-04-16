from __future__ import annotations

import html
import json
import sqlite3
import sys
import threading
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
import tkinter as tk
from tkinter import filedialog, messagebox, simpledialog, ttk

APP_DIR = Path(__file__).resolve().parent
APP_STATE = APP_DIR / "app_state.json"


class AppError(Exception):
    pass


@dataclass
class RepoPaths:
    root: Path
    env: Path
    db: Path


class EnvStore:
    def __init__(self, path: Path):
        self.path = path
        self.lines: list[str] = []
        self.values: dict[str, str] = {}
        self.load()

    def load(self) -> None:
        text = self.path.read_text(encoding="utf-8") if self.path.exists() else ""
        self.lines = text.splitlines()
        self.values = {}
        for line in self.lines:
            s = line.strip()
            if not s or s.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            self.values[key.strip()] = value.strip()

    def get(self, key: str, default: str = "") -> str:
        return self.values.get(key, default)

    def set(self, key: str, value: str) -> None:
        out: list[str] = []
        updated = False
        for line in self.lines:
            if "=" in line and not line.lstrip().startswith("#"):
                old_key = line.split("=", 1)[0].strip()
                if old_key == key:
                    out.append(f"{key}={value}")
                    updated = True
                    continue
            out.append(line)
        if not updated:
            if out and out[-1].strip():
                out.append("")
            out.append(f"{key}={value}")
        self.lines = out
        self.values[key] = value

    def save(self) -> None:
        text = "\n".join(self.lines)
        if text and not text.endswith("\n"):
            text += "\n"
        self.path.write_text(text, encoding="utf-8")


class Repo:
    def __init__(self, root: Path):
        root = root.resolve()
        env = root / ".env"
        db = root / "data" / "shop.db"
        if not root.exists():
            raise AppError(f"Không tồn tại thư mục: {root}")
        if not env.exists():
            raise AppError("Không thấy file .env")
        if not db.exists():
            raise AppError("Không thấy file data/shop.db")
        self.paths = RepoPaths(root=root, env=env, db=db)
        self.env = EnvStore(env)
        with self.connect() as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA foreign_keys=ON;")

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.paths.db)
        conn.row_factory = sqlite3.Row
        return conn

    def stats(self) -> dict[str, int]:
        with self.connect() as c:
            return {
                "users": int(c.execute("SELECT COUNT(*) FROM users").fetchone()[0] or 0),
                "stock": int(c.execute("SELECT COUNT(*) FROM stock WHERE is_sold = 0").fetchone()[0] or 0),
                "pending": int(c.execute("SELECT COUNT(*) FROM orders WHERE status = 'pending'").fetchone()[0] or 0),
                "delivered": int(c.execute("SELECT COUNT(*) FROM orders WHERE status = 'delivered'").fetchone()[0] or 0),
                "revenue": int(c.execute("SELECT COALESCE(SUM(total_price), 0) FROM orders WHERE status = 'delivered'").fetchone()[0] or 0),
            }

    def refresh_product_flags(self, product_id: int, conn: sqlite3.Connection | None = None) -> None:
        return

    def categories(self) -> list[sqlite3.Row]:
        with self.connect() as c:
            return c.execute("SELECT * FROM categories ORDER BY sort_order, id").fetchall()

    def categories_with_counts(self, keyword: str = "") -> list[sqlite3.Row]:
        sql = """
        SELECT c.*, COUNT(p.id) AS product_count
        FROM categories c
        LEFT JOIN products p ON p.category_id = c.id
        GROUP BY c.id, c.name, c.emoji, c.sort_order
        ORDER BY c.sort_order, c.id
        """
        with self.connect() as c:
            rows = c.execute(sql).fetchall()
        if not keyword.strip():
            return rows
        kw = keyword.lower().strip()
        return [r for r in rows if kw in str(r["name"] or "").lower() or kw in str(r["emoji"] or "").lower() or kw in str(r["id"])]

    def category(self, category_id: int) -> sqlite3.Row | None:
        with self.connect() as c:
            return c.execute("SELECT * FROM categories WHERE id=?", (category_id,)).fetchone()

    def add_category(self, name: str, emoji: str = "", sort_order: int = 99) -> int:
        name = name.strip()
        if not name:
            raise AppError("Tên danh mục không được trống")
        with self.connect() as c:
            cur = c.execute(
                "INSERT INTO categories (name, emoji, sort_order) VALUES (?, ?, ?)",
                (name, emoji.strip(), int(sort_order)),
            )
            c.commit()
            return int(cur.lastrowid)

    def update_category(self, category_id: int, name: str, emoji: str = "", sort_order: int = 99) -> None:
        if not self.category(category_id):
            raise AppError("Danh mục không tồn tại")
        name = name.strip()
        if not name:
            raise AppError("Tên danh mục không được trống")
        with self.connect() as c:
            c.execute(
                "UPDATE categories SET name=?, emoji=?, sort_order=? WHERE id=?",
                (name, emoji.strip(), int(sort_order), category_id),
            )
            c.commit()

    def delete_category(self, category_id: int, move_products_to: int | None = None) -> None:
        if not self.category(category_id):
            raise AppError("Danh mục không tồn tại")
        with self.connect() as c:
            count = int(c.execute("SELECT COUNT(*) FROM products WHERE category_id=?", (category_id,)).fetchone()[0] or 0)
            if count > 0:
                if move_products_to is None:
                    raise AppError("Danh mục còn sản phẩm. Hãy chọn danh mục đích để chuyển sản phẩm trước khi xóa.")
                if move_products_to == category_id:
                    raise AppError("Danh mục đích phải khác danh mục đang xóa")
                target = c.execute("SELECT id FROM categories WHERE id=?", (move_products_to,)).fetchone()
                if not target:
                    raise AppError("Danh mục đích không tồn tại")
                c.execute("UPDATE products SET category_id=? WHERE category_id=?", (move_products_to, category_id))
            c.execute("DELETE FROM categories WHERE id=?", (category_id,))
            c.commit()

    def products(self, keyword: str = "") -> list[sqlite3.Row]:
        sql = """
        SELECT
            p.id, p.category_id, p.name, p.price, p.description, p.emoji, p.promotion,
            p.contact_only, p.contact_url, p.sheet_stock, p.is_active,
            c.name AS category_name,
            (SELECT COUNT(*) FROM stock s WHERE s.product_id = p.id AND s.is_sold = 0) AS stock_real,
            CASE
                WHEN (SELECT COUNT(*) FROM stock s WHERE s.product_id = p.id AND s.is_sold = 0) > 0
                THEN (SELECT COUNT(*) FROM stock s WHERE s.product_id = p.id AND s.is_sold = 0)
                ELSE COALESCE(p.sheet_stock, 0)
            END AS stock_display
        FROM products p
        LEFT JOIN categories c ON c.id = p.category_id
        ORDER BY p.category_id, p.id
        """
        with self.connect() as c:
            rows = c.execute(sql).fetchall()
        if not keyword.strip():
            return rows
        kw = keyword.lower().strip()
        filtered = []
        for r in rows:
            text = " ".join([
                str(r["id"]),
                str(r["name"] or ""),
                str(r["category_name"] or ""),
                str(r["promotion"] or ""),
                str(r["description"] or ""),
            ]).lower()
            if kw in text:
                filtered.append(r)
        return filtered

    def product(self, product_id: int) -> sqlite3.Row | None:
        with self.connect() as c:
            return c.execute("SELECT * FROM products WHERE id=?", (product_id,)).fetchone()

    def add_product(self, data: dict[str, object]) -> int:
        name = str(data["name"]).strip()
        if not name:
            raise AppError("Tên sản phẩm không được trống")
        with self.connect() as c:
            cur = c.execute(
                """
                INSERT INTO products (
                    category_id, name, price, description, emoji, promotion,
                    contact_only, contact_url, sheet_stock, is_active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(data["category_id"]),
                    name,
                    int(data["price"]),
                    str(data.get("description", "")).strip() or None,
                    str(data.get("emoji", "")).strip(),
                    str(data.get("promotion", "")).strip() or None,
                    1 if data.get("contact_only") else 0,
                    str(data.get("contact_url", "")).strip() or None,
                    int(data.get("sheet_stock", 0) or 0),
                    1 if data.get("is_active", True) else 0,
                ),
            )
            product_id = int(cur.lastrowid)
            self.refresh_product_flags(product_id, c)
            c.commit()
            return product_id

    def update_product(self, product_id: int, data: dict[str, object]) -> None:
        if not self.product(product_id):
            raise AppError("Sản phẩm không tồn tại")
        with self.connect() as c:
            c.execute(
                """
                UPDATE products
                SET category_id=?, name=?, price=?, description=?, emoji=?, promotion=?,
                    contact_only=?, contact_url=?, sheet_stock=?, is_active=?
                WHERE id=?
                """,
                (
                    int(data["category_id"]),
                    str(data["name"]).strip(),
                    int(data["price"]),
                    str(data.get("description", "")).strip() or None,
                    str(data.get("emoji", "")).strip(),
                    str(data.get("promotion", "")).strip() or None,
                    1 if data.get("contact_only") else 0,
                    str(data.get("contact_url", "")).strip() or None,
                    int(data.get("sheet_stock", 0) or 0),
                    1 if data.get("is_active", True) else 0,
                    product_id,
                ),
            )
            self.refresh_product_flags(product_id, c)
            c.commit()

    def edit_name(self, product_id: int, name: str) -> None:
        if not name.strip():
            raise AppError("Tên không được trống")
        with self.connect() as c:
            c.execute("UPDATE products SET name=? WHERE id=?", (name.strip(), product_id))
            c.commit()

    def edit_price(self, product_id: int, price: int) -> None:
        with self.connect() as c:
            c.execute("UPDATE products SET price=? WHERE id=?", (int(price), product_id))
            c.commit()

    def toggle_product(self, product_id: int) -> bool:
        row = self.product(product_id)
        if not row:
            raise AppError("Sản phẩm không tồn tại")
        new_state = 0 if int(row["is_active"] or 0) else 1
        with self.connect() as c:
            c.execute("UPDATE products SET is_active=? WHERE id=?", (new_state, product_id))
            c.commit()
        return bool(new_state)

    def delete_product(self, product_id: int) -> None:
        row = self.product(product_id)
        if not row:
            raise AppError("Sản phẩm không tồn tại")
        with self.connect() as c:
            order_count = int(c.execute("SELECT COUNT(*) FROM orders WHERE product_id=?", (product_id,)).fetchone()[0] or 0)
            if order_count > 0:
                raise AppError("Không thể xóa sản phẩm đã phát sinh đơn hàng. Hãy tắt sản phẩm thay vì xóa.")
            c.execute("DELETE FROM stock WHERE product_id=? AND is_sold=0", (product_id,))
            c.execute("DELETE FROM products WHERE id=?", (product_id,))
            c.commit()

    def stock_items(self, product_id: int, keyword: str = "", limit: int = 1000) -> list[sqlite3.Row]:
        with self.connect() as c:
            rows = c.execute(
                "SELECT * FROM stock WHERE product_id=? AND is_sold=0 ORDER BY id LIMIT ?",
                (product_id, limit),
            ).fetchall()
        if not keyword.strip():
            return rows
        kw = keyword.lower().strip()
        return [r for r in rows if kw in str(r["data"] or "").lower() or kw in str(r["id"])]

    def stock_item(self, stock_id: int) -> sqlite3.Row | None:
        with self.connect() as c:
            return c.execute("SELECT * FROM stock WHERE id=?", (stock_id,)).fetchone()

    def add_stock(self, product_id: int, lines: list[str]) -> int:
        clean = [line.strip() for line in lines if line.strip()]
        if not clean:
            raise AppError("Không có dữ liệu kho")
        if not self.product(product_id):
            raise AppError("Sản phẩm không tồn tại")
        with self.connect() as c:
            c.executemany(
                "INSERT INTO stock (product_id, data) VALUES (?, ?)",
                [(product_id, line) for line in clean],
            )
            self.refresh_product_flags(product_id, c)
            c.commit()
        return len(clean)

    def delete_stock_item(self, stock_id: int) -> int:
        with self.connect() as c:
            row = c.execute("SELECT * FROM stock WHERE id=?", (stock_id,)).fetchone()
            if not row:
                raise AppError("Không tìm thấy dòng stock")
            if int(row["is_sold"] or 0):
                raise AppError("Không thể xóa dòng đã bán")
            product_id = int(row["product_id"])
            c.execute("DELETE FROM stock WHERE id=? AND is_sold=0", (stock_id,))
            self.refresh_product_flags(product_id, c)
            c.commit()
            return product_id

    def clear_stock(self, product_id: int) -> int:
        if not self.product(product_id):
            raise AppError("Sản phẩm không tồn tại")
        with self.connect() as c:
            cur = c.execute("DELETE FROM stock WHERE product_id=? AND is_sold=0", (product_id,))
            self.refresh_product_flags(product_id, c)
            c.commit()
            return int(cur.rowcount)

    def pending_orders(self, keyword: str = "") -> list[sqlite3.Row]:
        sql = """
        SELECT o.*, p.name AS product_name, COALESCE(u.full_name, u.username, CAST(u.telegram_id AS TEXT)) AS user_name
        FROM orders o
        JOIN products p ON p.id = o.product_id
        JOIN users u ON u.telegram_id = o.user_id
        WHERE o.status='pending'
        ORDER BY o.created_at ASC
        """
        with self.connect() as c:
            rows = c.execute(sql).fetchall()
        if not keyword.strip():
            return rows
        kw = keyword.lower().strip()
        return [r for r in rows if kw in " ".join([
            str(r["id"]),
            str(r["user_name"] or ""),
            str(r["product_name"] or ""),
            str(r["payment_code"] or ""),
        ]).lower()]

    def recent_orders(self, keyword: str = "", limit: int = 300) -> list[sqlite3.Row]:
        sql = """
        SELECT o.*, p.name AS product_name, COALESCE(u.full_name, u.username, CAST(u.telegram_id AS TEXT)) AS user_name
        FROM orders o
        JOIN products p ON p.id = o.product_id
        JOIN users u ON u.telegram_id = o.user_id
        ORDER BY o.created_at DESC
        LIMIT ?
        """
        with self.connect() as c:
            rows = c.execute(sql, (limit,)).fetchall()
        if not keyword.strip():
            return rows
        kw = keyword.lower().strip()
        return [r for r in rows if kw in " ".join([
            str(r["id"]),
            str(r["status"] or ""),
            str(r["user_name"] or ""),
            str(r["product_name"] or ""),
            str(r["payment_code"] or ""),
        ]).lower()]

    def order(self, order_id: int) -> sqlite3.Row | None:
        sql = """
        SELECT o.*, p.name AS product_name, p.sheet_stock, p.contact_only,
               COALESCE(u.full_name, u.username, CAST(u.telegram_id AS TEXT)) AS user_name
        FROM orders o
        JOIN products p ON p.id = o.product_id
        JOIN users u ON u.telegram_id = o.user_id
        WHERE o.id=?
        """
        with self.connect() as c:
            return c.execute(sql, (order_id,)).fetchone()

    def cancel_order(self, order_id: int) -> int:
        with self.connect() as c:
            cur = c.execute(
                "UPDATE orders SET status='cancelled' WHERE id=? AND status='pending'",
                (order_id,),
            )
            c.commit()
            return int(cur.rowcount)

    def confirm_order_auto(self, order_id: int) -> dict[str, object]:
        with self.connect() as c:
            order = c.execute("SELECT * FROM orders WHERE id=?", (order_id,)).fetchone()
            if not order:
                raise AppError("Đơn không tồn tại")
            if order["status"] != "pending":
                raise AppError(f"Đơn đang ở trạng thái {order['status']}")
            items = c.execute(
                "SELECT * FROM stock WHERE product_id=? AND is_sold=0 ORDER BY id LIMIT ?",
                (int(order["product_id"]), int(order["quantity"])),
            ).fetchall()
            if len(items) < int(order["quantity"]):
                raise AppError(f"Không đủ kho thật, hiện chỉ còn {len(items)}")
            c.executemany(
                "UPDATE stock SET is_sold=1, sold_to=?, sold_at=CURRENT_TIMESTAMP WHERE id=?",
                [(int(order["user_id"]), int(row["id"])) for row in items],
            )
            c.execute(
                """
                UPDATE orders
                SET status='delivered',
                    paid_at=COALESCE(paid_at, CURRENT_TIMESTAMP),
                    delivered_at=CURRENT_TIMESTAMP
                WHERE id=?
                """,
                (order_id,),
            )
            self.refresh_product_flags(int(order["product_id"]), c)
            c.commit()
        return {
            "order": self.order(order_id),
            "accounts": [str(r["data"]) for r in items],
        }

    def confirm_order_manual(self, order_id: int, accounts: list[str]) -> dict[str, object]:
        clean = [x.strip() for x in accounts if x.strip()]
        if not clean:
            raise AppError("Bạn chưa nhập dữ liệu giao tay")
        with self.connect() as c:
            order = c.execute("SELECT * FROM orders WHERE id=?", (order_id,)).fetchone()
            if not order:
                raise AppError("Đơn không tồn tại")
            if order["status"] != "pending":
                raise AppError(f"Đơn đang ở trạng thái {order['status']}")
            c.execute(
                """
                UPDATE orders
                SET status='delivered',
                    paid_at=COALESCE(paid_at, CURRENT_TIMESTAMP),
                    delivered_at=CURRENT_TIMESTAMP
                WHERE id=?
                """,
                (order_id,),
            )
            c.execute(
                "UPDATE products SET sheet_stock=MAX(COALESCE(sheet_stock, 0) - ?, 0) WHERE id=?",
                (int(order["quantity"]), int(order["product_id"])),
            )
            self.refresh_product_flags(int(order["product_id"]), c)
            c.commit()
        return {
            "order": self.order(order_id),
            "accounts": clean,
        }

    def users(self, keyword: str = "", limit: int = 1000) -> list[sqlite3.Row]:
        with self.connect() as c:
            rows = c.execute("SELECT * FROM users ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
        if not keyword.strip():
            return rows
        kw = keyword.lower().strip()
        return [r for r in rows if kw in " ".join([
            str(r["telegram_id"]),
            str(r["username"] or ""),
            str(r["full_name"] or ""),
        ]).lower()]

    def config(self) -> dict[str, str]:
        keys = [
            "BOT_TOKEN", "ADMIN_ID", "SHOP_NAME", "SUPPORT_CONTACT",
            "BANK_NAME", "BANK_BIN", "BANK_ACCOUNT", "BANK_ACCOUNT_NAME",
            "BANK2_NAME", "BANK2_BIN", "BANK2_ACCOUNT", "BANK2_ACCOUNT_NAME",
            "GOOGLE_SHEET_ID", "SHEET_SYNC_INTERVAL", "WEBHOOK_PORT", "SEPAY_API_KEY",
            "AUTO_REFRESH_MS",
        ]
        return {k: self.env.get(k, "") for k in keys}

    def save_config(self, values: dict[str, str]) -> None:
        for key, value in values.items():
            self.env.set(key, value)
        self.env.save()
        self.env.load()

    def bot_token(self) -> str:
        token = self.env.get("BOT_TOKEN", "").strip()
        if not token:
            raise AppError("BOT_TOKEN đang trống trong .env")
        return token

    def telegram_send(self, chat_id: int, text: str, parse_mode: str = "HTML") -> None:
        url = f"https://api.telegram.org/bot{self.bot_token()}/sendMessage"
        payload = urllib.parse.urlencode({
            "chat_id": str(chat_id),
            "text": text,
            "parse_mode": parse_mode,
        }).encode("utf-8")
        req = urllib.request.Request(url, data=payload, method="POST")
        with urllib.request.urlopen(req, timeout=20) as res:
            data = json.loads(res.read().decode("utf-8", errors="replace"))
        if not data.get("ok"):
            raise AppError(data.get("description") or "Telegram API lỗi")

    def send_delivery_messages(self, order: sqlite3.Row, accounts: list[str]) -> None:
        product_name = html.escape(str(order["product_name"]))
        quantity = int(order["quantity"])
        self.telegram_send(
            int(order["user_id"]),
            "✅ <b>Thanh toán thành công</b>\n\n"
            f"Đơn hàng của bạn đã được xác nhận: <b>{product_name}</b> × <b>{quantity}</b>.",
        )
        lines = [
            "✅ <b>ĐƠN HÀNG THÀNH CÔNG</b>",
            "",
            f"<b>Sản phẩm:</b> {product_name}",
            f"<b>Số lượng:</b> {quantity}",
            "",
            "<b>Thông tin tài khoản:</b>",
        ]
        for i, item in enumerate(accounts, 1):
            lines.append(f"{i}) <code>{html.escape(item)}</code>")
        lines.append("")
        lines.append("Cảm ơn bạn đã mua hàng.")
        self.telegram_send(int(order["user_id"]), "\n".join(lines))

    def broadcast(self, message: str) -> dict[str, int]:
        users = self.users(limit=200000)
        sent = 0
        failed = 0
        for user in users:
            try:
                self.telegram_send(int(user["telegram_id"]), message)
                sent += 1
            except Exception:
                failed += 1
        return {"total": len(users), "sent": sent, "failed": failed}


def format_price(value: int | str | None) -> str:
    try:
        n = int(value or 0)
    except Exception:
        n = 0
    return f"{n:,.0f}".replace(",", ".") + "đ"

def safe_int(value: str, default: int = 0) -> int:
    try:
        return int(str(value).strip())
    except Exception:
        return default

def normalize_contact_url(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""

    if text.startswith("@"):
        return f"https://t.me/{text[1:]}"
    if text.startswith("t.me/"):
        return f"https://{text}"
    return text


LIGHT_THEME = {
    "bg": "#F6F8FC",
    "panel": "#FFFFFF",
    "panel_soft": "#FAFBFD",
    "panel_alt": "#F1F5F9",
    "border": "#E2E8F0",
    "text": "#0F172A",
    "muted": "#64748B",
    "primary": "#2563EB",
    "primary_soft": "#E8F0FF",
    "primary_active": "#DCE7FF",
    "success": "#16A34A",
    "success_soft": "#ECFDF3",
    "danger": "#DC2626",
    "danger_soft": "#FEF2F2",
    "warning": "#D97706",
    "warning_soft": "#FFF7ED",
    "info": "#0891B2",
    "info_soft": "#ECFEFF",
    "input_bg": "#FFFFFF",
    "empty_bg": "#FFFFFF",
    "skeleton": "#E9EEF5",
    "tab_bg": "#EEF2F7",
    "tab_hover": "#E6EBF3",
    "tab_selected": "#FFFFFF",
    "button_bg": "#FFFFFF",
    "button_hover": "#F8FAFC",
}

DARK_THEME = {
    "bg": "#0F172A",
    "panel": "#111827",
    "panel_soft": "#1F2937",
    "border": "#334155",
    "text": "#E5E7EB",
    "muted": "#94A3B8",
    "primary": "#60A5FA",
    "primary_soft": "#1E3A8A",
    "success": "#4ADE80",
    "success_soft": "#14532D",
    "danger": "#F87171",
    "danger_soft": "#7F1D1D",
    "warning": "#F59E0B",
    "warning_soft": "#78350F",
    "info": "#22D3EE",
    "info_soft": "#164E63",
    "input_bg": "#0B1220",
    "empty_bg": "#0B1220",
    "skeleton": "#334155",
}


def order_status_label(status: str) -> str:
    mapping = {
        "pending": "Mới",
        "delivered": "Đã giao",
        "cancelled": "Hủy",
    }
    return mapping.get(str(status or "").lower(), str(status or "").title())


def bool_label(flag: object, true_text: str = "Bật", false_text: str = "Tắt") -> str:
    return true_text if int(flag or 0) else false_text


class ToastManager:
    def __init__(self, master: tk.Misc):
        self.master = master
        self._stack: list[tk.Toplevel] = []

    def show(self, message: str, kind: str = "info", duration: int = 2400) -> None:
        app = self.master.winfo_toplevel()
        if not hasattr(app, "theme"):
            return

        theme = app.theme
        if kind == "success":
            accent = theme["success"]
            bg = theme["success_soft"]
        elif kind == "error":
            accent = theme["danger"]
            bg = theme["danger_soft"]
        elif kind == "warning":
            accent = theme["warning"]
            bg = theme["warning_soft"]
        else:
            accent = theme["primary"]
            bg = theme["panel"]

        toast = tk.Toplevel(app)
        toast.overrideredirect(True)
        toast.attributes("-topmost", True)
        toast.configure(bg=theme["border"])

        body = tk.Frame(toast, bg=bg, padx=14, pady=10)
        body.pack(fill="both", expand=True)

        tk.Frame(body, bg=accent, width=4).pack(side="left", fill="y", padx=(0, 10))
        tk.Label(
            body,
            text=message,
            bg=bg,
            fg=theme["text"],
            font=("Segoe UI", 10, "bold"),
            justify="left",
            wraplength=320,
        ).pack(side="left")

        self._stack.append(toast)
        self._reposition()

        def close():
            if toast in self._stack:
                self._stack.remove(toast)
            try:
                toast.destroy()
            except Exception:
                pass
            self._reposition()

        toast.after(duration, close)

    def _reposition(self) -> None:
        app = self.master.winfo_toplevel()
        try:
            app.update_idletasks()
        except Exception:
            return

        base_x = app.winfo_rootx() + app.winfo_width() - 370
        base_y = app.winfo_rooty() + 24

        for i, toast in enumerate(self._stack):
            try:
                toast.update_idletasks()
                h = toast.winfo_reqheight()
                toast.geometry(f"340x{h}+{base_x}+{base_y + i * (h + 10)}")
            except Exception:
                pass


class LoadingButton(ttk.Button):
    def __init__(self, master: tk.Misc, text: str, command=None, **kwargs):
        super().__init__(master, text=text, command=command, **kwargs)
        self.default_text = text

    def set_loading(self, loading: bool, text: str = "Đang xử lý...") -> None:
        if loading:
            self.configure(text=f"⏳ {text}", state="disabled")
        else:
            self.configure(text=self.default_text, state="normal")


class EmptyState(tk.Frame):
    def __init__(self, master: tk.Misc, title: str = "Chưa có dữ liệu", desc: str = "Dữ liệu sẽ hiển thị tại đây"):
        app = master.winfo_toplevel()
        theme = getattr(app, "theme", LIGHT_THEME)

        super().__init__(master, bg=theme["panel"], bd=0, highlightthickness=0)

        self.theme = theme
        self.title_var = tk.StringVar(value=title)
        self.desc_var = tk.StringVar(value=desc)

        self.icon = tk.Label(
            self,
            text="◌",
            font=("Segoe UI Symbol", 22),
            bg=theme["panel"],
            fg=theme["muted"],
            bd=0,
            highlightthickness=0,
        )
        self.icon.pack(pady=(18, 6))

        self.title = tk.Label(
            self,
            textvariable=self.title_var,
            font=("Segoe UI", 14, "bold"),
            bg=theme["panel"],
            fg=theme["text"],
            bd=0,
            highlightthickness=0,
        )
        self.title.pack()

        self.desc = tk.Label(
            self,
            textvariable=self.desc_var,
            font=("Segoe UI", 10),
            bg=theme["panel"],
            fg=theme["muted"],
            bd=0,
            highlightthickness=0,
            wraplength=360,
            justify="center",
        )
        self.desc.pack(pady=(6, 0))

    def set_text(self, title: str, desc: str) -> None:
        self.title_var.set(title)
        self.desc_var.set(desc)

class ScrollableFrame(ttk.Frame):
    def __init__(self, master: tk.Misc, style: str = "Panel.TFrame"):
        super().__init__(master, style=style)

        self.canvas = tk.Canvas(self, bd=0, highlightthickness=0, relief="flat")
        self.v_scroll = ttk.Scrollbar(self, orient="vertical", command=self.canvas.yview)

        self.canvas.pack(side="left", fill="both", expand=True)
        self.v_scroll.pack(side="right", fill="y")

        self.canvas.configure(yscrollcommand=self.v_scroll.set)

        self.inner = ttk.Frame(self.canvas, style=style)
        self.window_id = self.canvas.create_window((0, 0), window=self.inner, anchor="nw")

        self.inner.bind("<Configure>", self._on_frame_configure)
        self.canvas.bind("<Configure>", self._on_canvas_configure)

        self.canvas.bind("<Enter>", self._bind_mousewheel)
        self.canvas.bind("<Leave>", self._unbind_mousewheel)

    def _on_frame_configure(self, _event=None):
        self.canvas.configure(scrollregion=self.canvas.bbox("all"))

    def _on_canvas_configure(self, event=None):
        if event:
            self.canvas.itemconfigure(self.window_id, width=event.width)

    def _bind_mousewheel(self, _event=None):
        self.canvas.bind_all("<MouseWheel>", self._on_mousewheel)

    def _unbind_mousewheel(self, _event=None):
        self.canvas.unbind_all("<MouseWheel>")

    def _on_mousewheel(self, event):
        try:
            self.canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
        except Exception:
            pass

    def sync_theme(self, theme: dict[str, str]) -> None:
        self.canvas.configure(bg=theme["panel"])

class ModernTable(ttk.Frame):
    def __init__(self, master: tk.Misc, columns: tuple[str, ...], headings: dict[str, str], height: int = 18):
        super().__init__(master)
        self.columns = columns
        self.headings = headings
        self._last_selected_first_value = None

        self.container = ttk.Frame(self, style="Panel.TFrame")
        self.container.pack(fill="both", expand=True)

        self.tree = ttk.Treeview(
            self.container,
            columns=columns,
            show="headings",
            selectmode="browse",
            height=height,
            style="Modern.Treeview",
        )

        y_scroll = ttk.Scrollbar(self.container, orient="vertical", command=self.tree.yview)
        x_scroll = ttk.Scrollbar(self.container, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=y_scroll.set, xscrollcommand=x_scroll.set)

        self.tree.grid(row=0, column=0, sticky="nsew")
        y_scroll.grid(row=0, column=1, sticky="ns")
        x_scroll.grid(row=1, column=0, sticky="ew")

        self.container.rowconfigure(0, weight=1)
        self.container.columnconfigure(0, weight=1)

        for col in columns:
            self.tree.heading(col, text=headings.get(col, col))
            self.tree.column(col, width=120, minwidth=80, anchor="w")

        self.tree.tag_configure("pending", foreground="#2563EB")
        self.tree.tag_configure("delivered", foreground="#16A34A")
        self.tree.tag_configure("cancelled", foreground="#DC2626")
        self.tree.tag_configure("inactive", foreground="#94A3B8")
        self.tree.tag_configure("skeleton", foreground="#94A3B8")

        self.tree.bind("<<TreeviewSelect>>", self._remember_selection)
        self.tree.bind("<ButtonRelease-1>", self._remember_click_selection)

        self.overlay = ttk.Frame(self.container, style="Panel.TFrame")
        self.empty_state = EmptyState(self.overlay)
        self.empty_state.pack(expand=True)

    def set_empty(self, title: str = "Chưa có dữ liệu", desc: str = "Kết quả sẽ xuất hiện tại đây") -> None:
        self.empty_state.set_text(title, desc)
        self.overlay.place(relx=0, rely=0, relwidth=1, relheight=1)

    def hide_empty(self) -> None:
        self.overlay.place_forget()

    def show_skeleton(self, rows: int = 8) -> None:
        self.clear()
        for i in range(rows):
            values = tuple("Đang tải..." if j == 1 else "…" for j, _ in enumerate(self.columns))
            self.tree.insert("", "end", values=values, tags=("skeleton",))
        self.hide_empty()

    def clear(self) -> None:
        for item in self.tree.get_children():
            self.tree.delete(item)

    def add(self, values: tuple, tags: tuple[str, ...] = ()) -> None:
        self.tree.insert("", "end", values=values, tags=tags)
        self.hide_empty()

    def selected_item(self):
        sel = self.tree.selection()
        if sel:
            return sel[0]

        focus = self.tree.focus()
        if focus:
            return focus

        return None

    def selected_value(self, index: int = 0) -> str | None:
        item = self.selected_item()
        if not item:
            return None
        values = self.tree.item(item, "values")
        if not values:
            return None
        return str(values[index])

    def get_selected_first_value(self) -> str | None:
        item = self.selected_item()
        if item:
            values = self.tree.item(item, "values")
            if values:
                return str(values[0])
        return self._last_selected_first_value

    def restore_selection_by_first_value(self, first_value: str | None) -> None:
        if not first_value:
            return
        for item in self.tree.get_children():
            values = self.tree.item(item, "values")
            if values and str(values[0]) == str(first_value):
                self.tree.selection_set(item)
                self.tree.focus(item)
                self.tree.see(item)
                self._last_selected_first_value = str(first_value)
                return

    def _remember_selection(self, _event=None):
        item = self.tree.focus()
        if not item:
            sel = self.tree.selection()
            if sel:
                item = sel[0]
        if item:
            values = self.tree.item(item, "values")
            if values:
                self._last_selected_first_value = str(values[0])

    def _remember_click_selection(self, event=None):
        if event is None:
            return
        item = self.tree.identify_row(event.y)
        if item:
            self.tree.selection_set(item)
            self.tree.focus(item)
            values = self.tree.item(item, "values")
            if values:
                self._last_selected_first_value = str(values[0])

class ProductDrawer(tk.Toplevel):
    def __init__(self, master: tk.Misc, repo: Repo, product: sqlite3.Row | None = None, theme: dict[str, str] | None = None):
        super().__init__(master)
        self.repo = repo
        self.product = product
        self.result: dict[str, object] | None = None
        self.theme = theme or LIGHT_THEME

        self.title("Sản phẩm")
        self.transient(master)
        self.grab_set()
        self.geometry("640x760")
        self.minsize(560, 620)
        self.configure(bg=self.theme["bg"])

        categories = self.repo.categories()
        if not categories:
            raise AppError("Chưa có danh mục. Hãy tạo ít nhất 1 danh mục trước.")

        self.category_map = {f"{c['id']} - {c['emoji']} {c['name']}": int(c["id"]) for c in categories}
        self.vars = {
            "name": tk.StringVar(),
            "price": tk.StringVar(value="0"),
            "description": tk.StringVar(),
            "emoji": tk.StringVar(),
            "promotion": tk.StringVar(),
            "contact_url": tk.StringVar(),
            "sheet_stock": tk.StringVar(value="0"),
            "contact_only": tk.BooleanVar(value=False),
            "is_active": tk.BooleanVar(value=True),
        }
        self.errors: dict[str, tk.StringVar] = {k: tk.StringVar(value="") for k in self.vars}
        self.contact_preview_var = tk.StringVar(value="")

        shell = ttk.Frame(self, style="Shell.TFrame")
        shell.pack(fill="both", expand=True)

        header = ttk.Frame(shell, style="Panel.TFrame", padding=18)
        header.pack(fill="x")

        ttk.Label(
            header,
            text="Chỉnh sửa sản phẩm" if product else "Thêm sản phẩm",
            style="DrawerTitle.TLabel",
        ).pack(anchor="w")
        ttk.Label(
            header,
            text="Form gọn, dễ nhìn, có cuộn và validate ngay tại chỗ",
            style="DrawerDesc.TLabel",
        ).pack(anchor="w", pady=(4, 0))

        content = ScrollableFrame(shell, style="Panel.TFrame")
        content.pack(fill="both", expand=True, padx=12, pady=(0, 0))
        content.sync_theme(self.theme)

        body = ttk.Frame(content.inner, style="Panel.TFrame", padding=18)
        body.pack(fill="both", expand=True)

        self.cbo = ttk.Combobox(body, values=list(self.category_map.keys()), state="readonly")
        self._field(body, "Danh mục", self.cbo, None, important=True)

        self.ent_name = ttk.Entry(body, textvariable=self.vars["name"])
        self._field(body, "Tên sản phẩm", self.ent_name, "name", important=True)

        self.ent_price = ttk.Entry(body, textvariable=self.vars["price"])
        self._field(body, "Giá", self.ent_price, "price", important=True)

        self.ent_desc = ttk.Entry(body, textvariable=self.vars["description"])
        self._field(body, "Mô tả", self.ent_desc, "description")

        self.ent_emoji = ttk.Entry(body, textvariable=self.vars["emoji"])
        self._field(body, "Emoji", self.ent_emoji, "emoji")

        self.ent_promo = ttk.Entry(body, textvariable=self.vars["promotion"])
        self._field(body, "Khuyến mãi", self.ent_promo, "promotion")

        self.ent_contact = ttk.Entry(body, textvariable=self.vars["contact_url"])
        self._field(body, "Link liên hệ hoặc @username", self.ent_contact, "contact_url")

        ttk.Label(body, textvariable=self.contact_preview_var, style="Muted.TLabel").pack(anchor="w", pady=(0, 10))

        self.ent_stock = ttk.Entry(body, textvariable=self.vars["sheet_stock"])
        self._field(body, "Sheet stock", self.ent_stock, "sheet_stock")

        switch_wrap = ttk.Frame(body, style="Panel.TFrame")
        switch_wrap.pack(fill="x", pady=(10, 0))

        ttk.Checkbutton(
            switch_wrap,
            text="Chế độ liên hệ",
            variable=self.vars["contact_only"],
            command=self.update_contact_preview,
        ).pack(side="left")

        ttk.Checkbutton(
            switch_wrap,
            text="Đang hoạt động",
            variable=self.vars["is_active"],
        ).pack(side="left", padx=(16, 0))

        footer = ttk.Frame(shell, style="Panel.TFrame", padding=18)
        footer.pack(fill="x", side="bottom")

        ttk.Button(footer, text="Hủy", command=self.destroy, style="Ghost.TButton").pack(side="right")
        self.save_btn = LoadingButton(footer, text="Lưu sản phẩm", command=self.on_save, style="Primary.TButton")
        self.save_btn.pack(side="right", padx=(0, 8))

        if product:
            current = next(
                (k for k, v in self.category_map.items() if v == int(product["category_id"] or 0)),
                list(self.category_map)[0],
            )
            self.cbo.set(current)
            self.vars["name"].set(str(product["name"] or ""))
            self.vars["price"].set(str(product["price"] or 0))
            self.vars["description"].set(str(product["description"] or ""))
            self.vars["emoji"].set(str(product["emoji"] or ""))
            self.vars["promotion"].set(str(product["promotion"] or ""))
            self.vars["contact_url"].set(str(product["contact_url"] or ""))
            self.vars["sheet_stock"].set(str(product["sheet_stock"] or 0))
            self.vars["contact_only"].set(bool(product["contact_only"]))
            self.vars["is_active"].set(bool(product["is_active"]))
        else:
            self.cbo.current(0)

        self.vars["name"].trace_add("write", lambda *_: self.validate_name())
        self.vars["price"].trace_add("write", lambda *_: self.validate_price())
        self.vars["sheet_stock"].trace_add("write", lambda *_: self.validate_stock())
        self.vars["contact_url"].trace_add("write", lambda *_: self.update_contact_preview())

        self.update_contact_preview()

        self.wait_visibility()
        self.focus_set()
        self.bind("<Return>", lambda _e: self.on_save())

    def _field(self, parent: ttk.Frame, label: str, widget, key: str | None, important: bool = False) -> None:
        wrap = ttk.Frame(parent, style="Panel.TFrame")
        wrap.pack(fill="x", pady=(0, 10))

        title = f"{label} *" if important else label
        ttk.Label(wrap, text=title, style="FieldLabel.TLabel").pack(anchor="w", pady=(0, 4))
        widget.pack(fill="x")

        if key and key in self.errors:
            ttk.Label(wrap, textvariable=self.errors[key], style="Error.TLabel").pack(anchor="w", pady=(4, 0))

    def update_contact_preview(self) -> None:
        raw = self.vars["contact_url"].get().strip()
        url = normalize_contact_url(raw)
        if self.vars["contact_only"].get():
            self.contact_preview_var.set(f"Preview liên hệ: {url or 'https://t.me/baoboifr'}")
        else:
            self.contact_preview_var.set("")

    def validate_name(self) -> bool:
        value = self.vars["name"].get().strip()
        self.errors["name"].set("" if value else "Tên sản phẩm không được để trống")
        return bool(value)

    def validate_price(self) -> bool:
        value = self.vars["price"].get().strip()
        if not value:
            self.errors["price"].set("Giá không được để trống")
            return False
        if not value.isdigit():
            self.errors["price"].set("Giá phải là số nguyên không âm")
            return False
        self.errors["price"].set("")
        return True

    def validate_stock(self) -> bool:
        value = self.vars["sheet_stock"].get().strip()
        if value and not value.isdigit():
            self.errors["sheet_stock"].set("Sheet stock phải là số nguyên không âm")
            return False
        self.errors["sheet_stock"].set("")
        return True

    def validate_all(self) -> bool:
        ok1 = self.validate_name()
        ok2 = self.validate_price()
        ok3 = self.validate_stock()
        return ok1 and ok2 and ok3

    def on_save(self) -> None:
        if not self.validate_all():
            return

        self.save_btn.set_loading(True, "Đang lưu...")
        try:
            contact_url = normalize_contact_url(self.vars["contact_url"].get().strip())
            if self.vars["contact_only"].get() and not contact_url:
                contact_url = "https://t.me/baoboifr"

            self.result = {
                "category_id": self.category_map[self.cbo.get()],
                "name": self.vars["name"].get().strip(),
                "price": safe_int(self.vars["price"].get(), 0),
                "description": self.vars["description"].get().strip(),
                "emoji": self.vars["emoji"].get().strip(),
                "promotion": self.vars["promotion"].get().strip(),
                "contact_url": contact_url,
                "sheet_stock": safe_int(self.vars["sheet_stock"].get(), 0),
                "contact_only": bool(self.vars["contact_only"].get()),
                "is_active": bool(self.vars["is_active"].get()),
            }
            self.destroy()
        finally:
            try:
                self.save_btn.set_loading(False)
            except Exception:
                pass

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Telegram Shop Admin Pro")
        self.geometry("1480x900")
        self.minsize(1220, 760)

        self.repo: Repo | None = None
        self.status_var = tk.StringVar(value="Chọn repo bot để bắt đầu")
        self.repo_var = tk.StringVar(value="Chưa mở repo")
        self.dark_mode_var = tk.BooleanVar(value=False)
        self.auto_refresh_ms = 4000

        self.category_search_var = tk.StringVar()
        self.product_search_var = tk.StringVar()
        self.stock_search_var = tk.StringVar()
        self.order_search_var = tk.StringVar()
        self.user_search_var = tk.StringVar()

        self.theme = LIGHT_THEME.copy()
        self.toast = ToastManager(self)

        self.configure(bg=self.theme["bg"])
        self.apply_style()
        self.build_ui()
        self.load_last_repo()
        self.after(self.auto_refresh_ms, self.auto_refresh_tick)

    def focus_products_tree(self):
        try:
            item = self.products_tree.selected_item()
            if item:
                self.products_tree.tree.selection_set(item)
                self.products_tree.tree.focus(item)
                self.products_tree.tree.see(item)
        except Exception:
            pass

    def focus_categories_tree(self):
        try:
            item = self.categories_tree.selected_item()
            if item:
                self.categories_tree.tree.selection_set(item)
                self.categories_tree.tree.focus(item)
                self.categories_tree.tree.see(item)
        except Exception:
            pass

    def apply_style(self) -> None:
        style = ttk.Style(self)
        try:
            style.theme_use("clam")
        except Exception:
            pass

        theme = self.theme
        self.configure(bg=theme["bg"])

        style.configure(".", font=("Segoe UI", 10))
        style.configure("Shell.TFrame", background=theme["bg"])
        style.configure("Panel.TFrame", background=theme["panel"])
        style.configure("Soft.TFrame", background=theme["panel_soft"])
        style.configure("Topbar.TFrame", background=theme["bg"])

        style.configure(
            "Title.TLabel",
            background=theme["bg"],
            foreground=theme["text"],
            font=("Segoe UI", 19, "bold"),
        )
        style.configure(
            "TopMuted.TLabel",
            background=theme["bg"],
            foreground=theme["muted"],
            font=("Segoe UI", 10),
        )
        style.configure(
            "SectionTitle.TLabel",
            background=theme["panel"],
            foreground=theme["text"],
            font=("Segoe UI", 12, "bold"),
        )
        style.configure(
            "Muted.TLabel",
            background=theme["panel"],
            foreground=theme["muted"],
            font=("Segoe UI", 10),
        )
        style.configure(
            "FieldLabel.TLabel",
            background=theme["panel"],
            foreground=theme["text"],
            font=("Segoe UI", 10, "bold"),
        )
        style.configure(
            "DrawerTitle.TLabel",
            background=theme["panel"],
            foreground=theme["text"],
            font=("Segoe UI", 18, "bold"),
        )
        style.configure(
            "DrawerDesc.TLabel",
            background=theme["panel"],
            foreground=theme["muted"],
            font=("Segoe UI", 10),
        )
        style.configure(
            "Error.TLabel",
            background=theme["panel"],
            foreground=theme["danger"],
            font=("Segoe UI", 9),
        )
        style.configure(
            "EmptyTitle.TLabel",
            background=theme["panel"],
            foreground=theme["text"],
            font=("Segoe UI", 12, "bold"),
        )
        style.configure(
            "EmptyDesc.TLabel",
            background=theme["panel"],
            foreground=theme["muted"],
            font=("Segoe UI", 10),
        )

        style.configure(
            "CardTitle.TLabel",
            background=theme["panel"],
            foreground=theme["muted"],
            font=("Segoe UI", 10, "bold"),
        )
        style.configure(
            "StatValue.TLabel",
            background=theme["panel"],
            foreground=theme["text"],
            font=("Segoe UI", 20, "bold"),
        )
        style.configure(
            "StatHint.TLabel",
            background=theme["panel"],
            foreground=theme["muted"],
            font=("Segoe UI", 9),
        )

        style.configure(
            "TNotebook",
            background=theme["bg"],
            borderwidth=0,
            tabmargins=(0, 0, 0, 0),
        )
        style.configure(
            "TNotebook.Tab",
            background=theme["tab_bg"],
            foreground=theme["text"],
            borderwidth=1,
            relief="flat",
            padding=(18, 10),
            font=("Segoe UI", 10, "bold"),
        )
        style.map(
            "TNotebook.Tab",
            background=[
                ("selected", theme["tab_selected"]),
                ("active", theme["tab_hover"]),
            ],
            foreground=[
                ("selected", theme["primary"]),
                ("active", theme["text"]),
            ],
            expand=[
                ("selected", (0, 0, 0, 0)),
                ("!selected", (0, 0, 0, 0)),
            ],
            padding=[
                ("selected", (18, 10)),
                ("!selected", (18, 10)),
            ],
        )

        style.configure(
            "TButton",
            background=theme["button_bg"],
            foreground=theme["text"],
            borderwidth=1,
            relief="flat",
            padding=(12, 8),
        )
        style.map(
            "TButton",
            background=[("active", theme["button_hover"])],
            relief=[("pressed", "flat"), ("active", "flat")],
        )

        style.configure(
            "Primary.TButton",
            background=theme["primary"],
            foreground="#FFFFFF",
            borderwidth=0,
            relief="flat",
            padding=(14, 9),
            font=("Segoe UI", 10, "bold"),
        )
        style.map(
            "Primary.TButton",
            background=[
                ("active", "#1D4ED8"),
                ("disabled", theme["border"]),
            ],
            foreground=[
                ("!disabled", "#FFFFFF"),
                ("disabled", "#94A3B8"),
            ],
            relief=[("pressed", "flat"), ("active", "flat")],
        )

        style.configure(
            "Ghost.TButton",
            background=theme["button_bg"],
            foreground=theme["text"],
            borderwidth=1,
            relief="flat",
            padding=(12, 8),
        )
        style.map(
            "Ghost.TButton",
            background=[("active", theme["button_hover"])],
            relief=[("pressed", "flat"), ("active", "flat")],
        )

        style.configure(
            "TCheckbutton",
            background=theme["bg"],
            foreground=theme["text"],
        )
        style.map(
            "TCheckbutton",
            background=[("active", theme["bg"])],
            foreground=[("active", theme["text"])],
        )

        style.configure(
            "TEntry",
            fieldbackground=theme["input_bg"],
            background=theme["input_bg"],
            foreground=theme["text"],
            insertcolor=theme["text"],
            bordercolor=theme["border"],
            lightcolor=theme["border"],
            darkcolor=theme["border"],
            padding=8,
            relief="flat",
        )
        style.map(
            "TEntry",
            bordercolor=[("focus", theme["primary"])],
            lightcolor=[("focus", theme["primary"])],
            darkcolor=[("focus", theme["primary"])],
        )

        style.configure(
            "TCombobox",
            fieldbackground=theme["input_bg"],
            background=theme["input_bg"],
            foreground=theme["text"],
            bordercolor=theme["border"],
            lightcolor=theme["border"],
            darkcolor=theme["border"],
            padding=8,
            relief="flat",
        )
        style.map(
            "TCombobox",
            bordercolor=[("focus", theme["primary"])],
            lightcolor=[("focus", theme["primary"])],
            darkcolor=[("focus", theme["primary"])],
        )

        style.configure(
            "TLabelframe",
            background=theme["panel"],
            borderwidth=1,
            relief="solid",
            bordercolor=theme["border"],
            lightcolor=theme["border"],
            darkcolor=theme["border"],
        )
        style.configure(
            "TLabelframe.Label",
            background=theme["panel"],
            foreground=theme["text"],
            font=("Segoe UI", 10, "bold"),
        )

        style.configure(
            "Stat.TLabelframe",
            background=theme["panel"],
            borderwidth=1,
            relief="solid",
            bordercolor=theme["border"],
            lightcolor=theme["border"],
            darkcolor=theme["border"],
        )
        style.configure(
            "Stat.TLabelframe.Label",
            background=theme["panel"],
            foreground=theme["muted"],
            font=("Segoe UI", 10, "bold"),
        )

        style.configure(
            "Modern.Treeview",
            background=theme["panel"],
            fieldbackground=theme["panel"],
            foreground=theme["text"],
            rowheight=36,
            borderwidth=0,
            relief="flat",
            font=("Segoe UI", 10),
        )
        style.configure(
            "Modern.Treeview.Heading",
            background=theme["panel_soft"],
            foreground=theme["muted"],
            font=("Segoe UI", 10, "bold"),
            relief="flat",
            padding=10,
        )
        style.map(
            "Modern.Treeview",
            background=[("selected", theme["primary_soft"])],
            foreground=[("selected", theme["text"])],
        )

    def toggle_theme(self) -> None:
        self.theme = DARK_THEME.copy() if self.dark_mode_var.get() else LIGHT_THEME.copy()
        self.apply_style()
        self.build_ui()
        if self.repo:
            self.refresh_all()

    def show_toast(self, message: str, kind: str = "info") -> None:
        self.toast.show(message, kind=kind)

    def set_busy(self, busy: bool) -> None:
        self.configure(cursor="watch" if busy else "")
        try:
            self.update_idletasks()
        except Exception:
            pass

    def build_ui(self) -> None:
        for child in self.winfo_children():
            child.destroy()

        root = ttk.Frame(self, style="Shell.TFrame", padding=12)
        root.pack(fill="both", expand=True)

        top = ttk.Frame(root, style="Topbar.TFrame")
        top.pack(fill="x", pady=(0, 10))

        left = ttk.Frame(top, style="Topbar.TFrame")
        left.pack(side="left", fill="x", expand=True)

        ttk.Label(left, text="Telegram Shop Admin Pro", style="Title.TLabel").pack(anchor="w")
        ttk.Label(
            left,
            textvariable=self.repo_var,
            style="TopMuted.TLabel",
        ).pack(anchor="w", pady=(4, 0))

        right = ttk.Frame(top, style="Topbar.TFrame")
        right.pack(side="right")

        ttk.Checkbutton(
            right,
            text="Dark mode",
            variable=self.dark_mode_var,
            command=self.toggle_theme,
        ).pack(side="right", padx=(12, 0))

        ttk.Button(right, text="Nạp lại", command=self.refresh_all).pack(side="right", padx=(8, 0))
        ttk.Button(right, text="Chọn repo bot", command=self.choose_repo, style="Primary.TButton").pack(side="right")

        self.notebook = ttk.Notebook(root)
        self.notebook.pack(fill="both", expand=True, pady=(2, 0))

        self.tab_dashboard = ttk.Frame(self.notebook, padding=14, style="Panel.TFrame")
        self.tab_categories = ttk.Frame(self.notebook, padding=14, style="Panel.TFrame")
        self.tab_products = ttk.Frame(self.notebook, padding=14, style="Panel.TFrame")
        self.tab_stock = ttk.Frame(self.notebook, padding=14, style="Panel.TFrame")
        self.tab_orders = ttk.Frame(self.notebook, padding=14, style="Panel.TFrame")
        self.tab_users = ttk.Frame(self.notebook, padding=14, style="Panel.TFrame")
        self.tab_settings = ttk.Frame(self.notebook, padding=14, style="Panel.TFrame")

        self.notebook.add(self.tab_dashboard, text="Tổng quan")
        self.notebook.add(self.tab_categories, text="Danh mục")
        self.notebook.add(self.tab_products, text="Sản phẩm")
        self.notebook.add(self.tab_stock, text="Kho")
        self.notebook.add(self.tab_orders, text="Đơn hàng")
        self.notebook.add(self.tab_users, text="Users & Broadcast")
        self.notebook.add(self.tab_settings, text="Cài đặt")

        self.build_dashboard_tab()
        self.build_categories_tab()
        self.build_products_tab()
        self.build_stock_tab()
        self.build_orders_tab()
        self.build_users_tab()
        self.build_settings_tab()

        bottom = ttk.Frame(root, style="Panel.TFrame", padding=(12, 10))
        bottom.pack(fill="x", pady=(10, 0))
        ttk.Label(bottom, textvariable=self.status_var, style="Muted.TLabel").pack(side="left", anchor="w")

    def set_status(self, text: str) -> None:
        self.status_var.set(text)
        self.update_idletasks()

    def choose_repo(self) -> None:
        path = filedialog.askdirectory(title="Chọn thư mục gốc telegram-shop-bot")
        if path:
            self.open_repo(path)

    def save_last_repo(self, root: Path) -> None:
        APP_STATE.write_text(
            json.dumps({"repo_root": str(root)}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def load_last_repo(self) -> None:
        if not APP_STATE.exists():
            return
        try:
            payload = json.loads(APP_STATE.read_text(encoding="utf-8"))
            root = payload.get("repo_root")
            if root:
                self.open_repo(root)
        except Exception:
            pass

    def current_repo(self) -> Repo:
        if not self.repo:
            raise AppError("Bạn chưa mở repo bot")
        return self.repo

    def open_repo(self, path: str) -> None:
        try:
            self.repo = Repo(Path(path))
            self.repo_var.set(f"Repo: {self.repo.paths.root}")
            self.save_last_repo(self.repo.paths.root)

            auto_ms = self.repo.env.get("AUTO_REFRESH_MS", "").strip()
            if auto_ms.isdigit():
                self.auto_refresh_ms = max(2000, int(auto_ms))

            self.refresh_all()
            self.set_status("Đã mở repo thành công")
        except Exception as exc:
            self.repo = None
            messagebox.showerror("Lỗi mở repo", str(exc), parent=self)
            self.set_status("Mở repo thất bại")

    def run_bg(self, title: str, func, done=None) -> None:
        self.set_busy(True)
        self.set_status(f"{title} đang chạy...")

        def worker():
            try:
                result = func()
                self.after(0, lambda: self.finish_bg(title, result, done))
            except Exception as exc:
                self.after(0, lambda: self.fail_bg(title, exc))

        threading.Thread(target=worker, daemon=True).start()

    def finish_bg(self, title: str, result, done=None) -> None:
        self.set_busy(False)
        if done:
            done(result)
        self.set_status(f"{title}: hoàn tất")

    def fail_bg(self, title: str, exc: Exception) -> None:
        self.set_busy(False)
        self.show_toast(f"{title} thất bại: {exc}", "error")
        messagebox.showerror(title, str(exc), parent=self)
        self.set_status(f"{title}: thất bại")

    def auto_refresh_tick(self) -> None:
        try:
            if self.repo:
                self.refresh_dashboard()
                self.refresh_categories(silent=True)
                self.refresh_products(silent=True)
                self.refresh_orders(silent=True)

                stock_pid = self.stock_product_id_var.get().strip()
                if stock_pid.isdigit():
                    self.load_stock(silent=True)
        except Exception:
            pass
        finally:
            self.after(self.auto_refresh_ms, self.auto_refresh_tick)

    def build_dashboard_tab(self) -> None:
        header = ttk.Frame(self.tab_dashboard, style="Panel.TFrame")
        header.pack(fill="x")

        ttk.Label(header, text="Dashboard quản trị", style="SectionTitle.TLabel").pack(side="left")
        ttk.Button(header, text="Làm mới", command=self.refresh_dashboard).pack(side="right")

        cards = ttk.Frame(self.tab_dashboard, style="Panel.TFrame")
        cards.pack(fill="x", pady=(14, 14))

        self.stat_labels: dict[str, ttk.Label] = {}
        stats = [
            ("users", "Users"),
            ("delivered", "Đơn hoàn thành"),
            ("revenue", "Doanh thu"),
            ("pending", "Đơn chờ"),
            ("stock", "Kho thật"),
        ]

        for i, (key, title) in enumerate(stats):
            cards.columnconfigure(i, weight=1, uniform="dashboard_stat")

            box = ttk.LabelFrame(cards, text=title, style="Stat.TLabelframe", padding=16)
            box.grid(row=0, column=i, sticky="nsew", padx=6)

            inner = ttk.Frame(box, style="Panel.TFrame")
            inner.pack(fill="both", expand=True)

            value_label = ttk.Label(inner, text="-", style="StatValue.TLabel")
            value_label.pack(anchor="w", pady=(2, 2))

            ttk.Label(inner, text="Cập nhật theo dữ liệu hiện tại", style="StatHint.TLabel").pack(anchor="w")

            self.stat_labels[key] = value_label

        pane = ttk.Panedwindow(self.tab_dashboard, orient="horizontal")
        pane.pack(fill="both", expand=True)

        left = ttk.Labelframe(pane, text="Đơn chờ", padding=10)
        right = ttk.Labelframe(pane, text="Sản phẩm", padding=10)
        pane.add(left, weight=1)
        pane.add(right, weight=1)

        self.dashboard_pending_tree = ModernTable(
            left,
            ("id", "user", "product", "qty", "total", "created"),
            {
                "id": "Order ID",
                "user": "Khách",
                "product": "Sản phẩm",
                "qty": "SL",
                "total": "Tổng",
                "created": "Tạo lúc",
            },
            height=14,
        )
        self.dashboard_pending_tree.pack(fill="both", expand=True)

        self.dashboard_products_tree = ModernTable(
            right,
            ("id", "category", "name", "price", "stock", "active"),
            {
                "id": "ID",
                "category": "Danh mục",
                "name": "Tên",
                "price": "Giá",
                "stock": "Kho hiển thị",
                "active": "Trạng thái",
            },
            height=14,
        )
        self.dashboard_products_tree.pack(fill="both", expand=True)

    def build_categories_tab(self) -> None:
        bar = ttk.Frame(self.tab_categories, style="Panel.TFrame")
        bar.pack(fill="x", pady=(0, 10))

        ttk.Button(bar, text="Làm mới", command=self.refresh_categories).pack(side="left")
        ttk.Button(bar, text="Thêm danh mục", command=self.add_category_dialog, style="Primary.TButton").pack(side="left", padx=6)
        ttk.Button(bar, text="Sửa", command=self.edit_category_dialog).pack(side="left", padx=6)
        ttk.Button(bar, text="Xóa", command=self.delete_category_dialog).pack(side="left", padx=6)

        ttk.Label(bar, text="Tìm kiếm").pack(side="left", padx=(18, 6))
        entry = ttk.Entry(bar, textvariable=self.category_search_var, width=28)
        entry.pack(side="left")
        entry.bind("<KeyRelease>", lambda _e: self.refresh_categories(silent=True))

        self.categories_tree = ModernTable(
            self.tab_categories,
            ("id", "emoji", "name", "products", "sort_order"),
            {
                "id": "ID",
                "emoji": "Icon",
                "name": "Tên danh mục",
                "products": "Số sản phẩm",
                "sort_order": "Sort",
            },
        )
        self.categories_tree.pack(fill="both", expand=True)
        self.categories_tree.tree.bind("<Double-1>", lambda _e: self.edit_category_dialog())

    def build_products_tab(self) -> None:
        bar = ttk.Frame(self.tab_products, style="Panel.TFrame")
        bar.pack(fill="x", pady=(0, 10))

        ttk.Button(bar, text="Làm mới", command=self.refresh_products).pack(side="left")
        ttk.Button(bar, text="Thêm sản phẩm", command=self.add_product_dialog, style="Primary.TButton").pack(side="left", padx=6)

        ttk.Label(bar, text="Tìm kiếm").pack(side="left", padx=(18, 6))
        entry = ttk.Entry(bar, textvariable=self.product_search_var, width=30)
        entry.pack(side="left")
        entry.bind("<KeyRelease>", lambda _e: self.refresh_products(silent=True))

        self.products_tree = ModernTable(
            self.tab_products,
            (
                "id",
                "category",
                "name",
                "price",
                "stock_real",
                "stock_display",
                "sheet_stock",
                "active",
                "contact_only",
                "promotion",
            ),
            {
                "id": "ID",
                "category": "Danh mục",
                "name": "Tên sản phẩm",
                "price": "Giá",
                "stock_real": "Kho thật",
                "stock_display": "Kho hiển thị",
                "sheet_stock": "Sheet stock",
                "active": "Trạng thái",
                "contact_only": "Liên hệ",
                "promotion": "Khuyến mãi",
            },
        )
        self.products_tree.pack(fill="both", expand=True)
        self.products_tree.tree.bind("<Double-1>", lambda _e: self.edit_product_dialog())
        self.products_tree.tree.bind("<Button-3>", self.show_products_context_menu)

        self.products_context_menu = tk.Menu(self, tearoff=0)
        self.products_context_menu.add_command(label="Sửa sản phẩm", command=self.edit_product_dialog)
        self.products_context_menu.add_command(label="Đổi tên", command=self.quick_edit_name)
        self.products_context_menu.add_command(label="Đổi giá", command=self.quick_edit_price)
        self.products_context_menu.add_separator()
        self.products_context_menu.add_command(label="Bật / Tắt", command=self.toggle_product)
        self.products_context_menu.add_command(label="Xóa", command=self.delete_product)

    def build_orders_tab(self) -> None:
        bar = ttk.Frame(self.tab_orders, style="Panel.TFrame")
        bar.pack(fill="x", pady=(0, 10))

        ttk.Button(bar, text="Làm mới", command=self.refresh_orders).pack(side="left")

        order_btn = ttk.Menubutton(bar, text="⋯ Thao tác đơn")
        order_menu = tk.Menu(order_btn, tearoff=0)
        order_menu.add_command(label="Xác nhận tự động", command=self.confirm_auto)
        order_menu.add_command(label="Xác nhận giao tay", command=self.confirm_manual)
        order_menu.add_separator()
        order_menu.add_command(label="Hủy đơn", command=self.cancel_order)
        order_btn["menu"] = order_menu
        order_btn.pack(side="left", padx=6)

        ttk.Label(bar, text="Tìm kiếm").pack(side="left", padx=(18, 6))
        entry = ttk.Entry(bar, textvariable=self.order_search_var, width=30)
        entry.pack(side="left")
        entry.bind("<KeyRelease>", lambda _e: self.refresh_orders(silent=True))

        pane = ttk.Panedwindow(self.tab_orders, orient="vertical")
        pane.pack(fill="both", expand=True)

        a = ttk.Labelframe(pane, text="Đơn chờ", padding=10)
        b = ttk.Labelframe(pane, text="Tất cả đơn gần đây", padding=10)
        pane.add(a, weight=1)
        pane.add(b, weight=1)

        self.pending_tree = ModernTable(
            a,
            ("id", "status", "user", "product", "qty", "total", "payment_code", "created"),
            {
                "id": "Order ID",
                "status": "Badge",
                "user": "Khách",
                "product": "Sản phẩm",
                "qty": "SL",
                "total": "Tổng",
                "payment_code": "Payment code",
                "created": "Tạo lúc",
            },
            height=10,
        )
        self.pending_tree.pack(fill="both", expand=True)

        self.orders_tree = ModernTable(
            b,
            ("id", "status", "user", "product", "qty", "total", "created", "delivered"),
            {
                "id": "Order ID",
                "status": "Badge",
                "user": "Khách",
                "product": "Sản phẩm",
                "qty": "SL",
                "total": "Tổng",
                "created": "Tạo lúc",
                "delivered": "Giao lúc",
            },
            height=12,
        )
        self.orders_tree.pack(fill="both", expand=True)

    def build_users_tab(self) -> None:
        pane = ttk.Panedwindow(self.tab_users, orient="horizontal")
        pane.pack(fill="both", expand=True)

        left = ttk.Labelframe(pane, text="Users", padding=10)
        right = ttk.Labelframe(pane, text="Broadcast", padding=10)
        pane.add(left, weight=1)
        pane.add(right, weight=1)

        toolbar = ttk.Frame(left, style="Panel.TFrame")
        toolbar.pack(fill="x", pady=(0, 10))

        ttk.Button(toolbar, text="Làm mới users", command=self.refresh_users).pack(side="left")
        ttk.Label(toolbar, text="Tìm kiếm").pack(side="left", padx=(18, 6))

        entry = ttk.Entry(toolbar, textvariable=self.user_search_var, width=30)
        entry.pack(side="left")
        entry.bind("<KeyRelease>", lambda _e: self.refresh_users(silent=True))

        self.users_tree = ModernTable(
            left,
            ("telegram_id", "username", "full_name", "balance", "created_at"),
            {
                "telegram_id": "Telegram ID",
                "username": "Username",
                "full_name": "Tên",
                "balance": "Balance",
                "created_at": "Tạo lúc",
            },
        )
        self.users_tree.pack(fill="both", expand=True)

        ttk.Label(right, text="Nội dung broadcast (hỗ trợ HTML):", style="SectionTitle.TLabel").pack(anchor="w")
        ttk.Label(right, text="Gửi thông báo cho toàn bộ user với loading + toast phản hồi", style="Muted.TLabel").pack(anchor="w", pady=(2, 8))

        self.broadcast_text = tk.Text(
            right,
            height=18,
            wrap="word",
            bd=0,
            relief="flat",
            font=("Segoe UI", 10),
            bg=self.theme["input_bg"],
            fg=self.theme["text"],
            insertbackground=self.theme["text"],
        )
        self.broadcast_text.pack(fill="both", expand=True, pady=(0, 10))

        action = ttk.Frame(right, style="Panel.TFrame")
        action.pack(fill="x")
        self.broadcast_btn = LoadingButton(action, text="Gửi broadcast", command=self.send_broadcast, style="Primary.TButton")
        self.broadcast_btn.pack(side="right")

    def build_settings_tab(self) -> None:
        self.settings_vars: dict[str, tk.StringVar] = {}

        fields = [
            ("BOT_TOKEN", "Bot token"),
            ("ADMIN_ID", "Admin Telegram ID"),
            ("SHOP_NAME", "Tên shop"),
            ("SUPPORT_CONTACT", "Liên hệ hỗ trợ"),
            ("BANK_NAME", "Tên ngân hàng 1"),
            ("BANK_BIN", "BIN ngân hàng 1"),
            ("BANK_ACCOUNT", "Số tài khoản 1"),
            ("BANK_ACCOUNT_NAME", "Chủ tài khoản 1"),
            ("BANK2_NAME", "Tên ngân hàng 2"),
            ("BANK2_BIN", "BIN ngân hàng 2"),
            ("BANK2_ACCOUNT", "Số tài khoản 2"),
            ("BANK2_ACCOUNT_NAME", "Chủ tài khoản 2"),
            ("GOOGLE_SHEET_ID", "Google Sheet ID"),
            ("SHEET_SYNC_INTERVAL", "Phút auto sync"),
            ("AUTO_REFRESH_MS", "Auto refresh ms"),
            ("WEBHOOK_PORT", "Webhook port"),
            ("SEPAY_API_KEY", "SEPAY API key"),
        ]

        shell = ttk.Frame(self.tab_settings, style="Panel.TFrame")
        shell.pack(fill="both", expand=True)

        scroll = ScrollableFrame(shell, style="Panel.TFrame")
        scroll.pack(fill="both", expand=True)
        scroll.sync_theme(self.theme)

        wrap = ttk.Frame(scroll.inner, style="Panel.TFrame", padding=8)
        wrap.pack(fill="both", expand=True)

        header = ttk.Frame(wrap, style="Panel.TFrame")
        header.pack(fill="x", pady=(0, 12))

        ttk.Label(header, text="Cấu hình hệ thống", style="SectionTitle.TLabel").pack(anchor="w")
        ttk.Label(header, text="Giữ nguyên logic .env, chỉ nâng cấp form hiển thị", style="Muted.TLabel").pack(anchor="w", pady=(2, 0))

        form = ttk.Frame(wrap, style="Panel.TFrame")
        form.pack(fill="x", expand=True)

        for i, (key, label) in enumerate(fields):
            ttk.Label(
                form,
                text=label,
                style="FieldLabel.TLabel",
            ).grid(row=i, column=0, sticky="nw", pady=6, padx=(0, 14))

            var = tk.StringVar()
            self.settings_vars[key] = var

            ttk.Entry(
                form,
                textvariable=var,
                width=88,
            ).grid(row=i, column=1, sticky="ew", pady=6)

        form.columnconfigure(1, weight=1)

        btns = ttk.Frame(wrap, style="Panel.TFrame")
        btns.pack(fill="x", pady=(12, 0))

        ttk.Button(btns, text="Nạp lại .env", command=self.load_settings).pack(side="left")
        ttk.Button(btns, text="Lưu .env", command=self.save_settings, style="Primary.TButton").pack(side="left", padx=6)

    def refresh_all(self) -> None:
        if not self.repo:
            return
        self.refresh_dashboard()
        self.refresh_categories(silent=True)
        self.refresh_products(silent=True)
        self.refresh_orders(silent=True)
        self.refresh_users(silent=True)
        self.load_settings()


    def refresh_dashboard(self) -> None:
        repo = self.current_repo()
        data = repo.stats()

        for key, label in self.stat_labels.items():
            label.configure(text=format_price(data[key]) if key == "revenue" else str(data[key]))

        self.dashboard_pending_tree.show_skeleton(5)
        self.dashboard_pending_tree.clear()
        pending_rows = repo.pending_orders()[:20]
        if not pending_rows:
            self.dashboard_pending_tree.set_empty("Chưa có đơn chờ", "Các đơn pending sẽ xuất hiện ở đây")
        else:
            for row in pending_rows:
                self.dashboard_pending_tree.add((
                    row["id"],
                    row["user_name"],
                    row["product_name"],
                    row["quantity"],
                    format_price(row["total_price"]),
                    row["created_at"],
                ), tags=("pending",))

        self.dashboard_products_tree.show_skeleton(5)
        self.dashboard_products_tree.clear()
        product_rows = repo.products()[:30]
        if not product_rows:
            self.dashboard_products_tree.set_empty("Chưa có sản phẩm", "Hãy thêm sản phẩm đầu tiên của bạn")
        else:
            for row in product_rows:
                tags = ()
                if not int(row["is_active"] or 0):
                    tags = ("inactive",)
                self.dashboard_products_tree.add((
                    row["id"],
                    row["category_name"] or "",
                    row["name"],
                    format_price(row["price"]),
                    row["stock_display"],
                    bool_label(row["is_active"], "Bật", "Tắt"),
                ), tags=tags)


    def refresh_categories(self, silent: bool = False) -> None:
        repo = self.current_repo()
        selected_id = self.categories_tree.get_selected_first_value()

        self.categories_tree.show_skeleton(6)
        self.categories_tree.clear()

        rows = repo.categories_with_counts(self.category_search_var.get())
        if not rows:
            self.categories_tree.set_empty("Không có danh mục", "Thử đổi từ khóa hoặc thêm danh mục mới")
        else:
            for row in rows:
                self.categories_tree.add((
                    row["id"],
                    row["emoji"] or "",
                    row["name"],
                    row["product_count"],
                    row["sort_order"],
                ))

        self.categories_tree.restore_selection_by_first_value(selected_id)

        if not silent:
            self.set_status("Đã tải danh mục")


    def refresh_products(self, silent: bool = False) -> None:
        repo = self.current_repo()
        selected_id = self.products_tree.get_selected_first_value()

        self.products_tree.show_skeleton(8)
        self.products_tree.clear()

        rows = repo.products(self.product_search_var.get())
        if not rows:
            self.products_tree.set_empty("Không có sản phẩm", "Danh sách sản phẩm sẽ hiển thị tại đây")
        else:
            for row in rows:
                tags = ()
                if not int(row["is_active"] or 0):
                    tags = ("inactive",)

                self.products_tree.add((
                    row["id"],
                    f"{row['category_id']} - {row['category_name'] or ''}",
                    row["name"],
                    format_price(row["price"]),
                    row["stock_real"],
                    row["stock_display"],
                    row["sheet_stock"],
                    bool_label(row["is_active"], "Bật", "Tắt"),
                    bool_label(row["contact_only"], "Liên hệ", "Bán trực tiếp"),
                    row["promotion"] or "",
                ), tags=tags)

        self.products_tree.restore_selection_by_first_value(selected_id)

        if not silent:
            self.set_status("Đã tải sản phẩm")


    def refresh_orders(self, silent: bool = False) -> None:
        repo = self.current_repo()
        keyword = self.order_search_var.get()

        pending_selected_id = self.pending_tree.get_selected_first_value()
        order_selected_id = self.orders_tree.get_selected_first_value()

        self.pending_tree.show_skeleton(5)
        self.pending_tree.clear()
        pending_rows = repo.pending_orders(keyword)
        if not pending_rows:
            self.pending_tree.set_empty("Không có đơn chờ", "Mọi đơn chờ xác nhận sẽ hiển thị ở đây")
        else:
            for row in pending_rows:
                self.pending_tree.add((
                    row["id"],
                    "🔵 Mới",
                    row["user_name"],
                    row["product_name"],
                    row["quantity"],
                    format_price(row["total_price"]),
                    row["payment_code"],
                    row["created_at"],
                ), tags=("pending",))
        self.pending_tree.restore_selection_by_first_value(pending_selected_id)

        self.orders_tree.show_skeleton(8)
        self.orders_tree.clear()
        recent_rows = repo.recent_orders(keyword)
        if not recent_rows:
            self.orders_tree.set_empty("Chưa có đơn hàng", "Lịch sử đơn hàng sẽ hiển thị tại đây")
        else:
            for row in recent_rows:
                status = str(row["status"] or "").lower()
                tags = (status,) if status in {"pending", "delivered", "cancelled"} else ()
                badge = {
                    "pending": "🔵 Mới",
                    "delivered": "🟢 Đã giao",
                    "cancelled": "🔴 Hủy",
                }.get(status, status.title())
                self.orders_tree.add((
                    row["id"],
                    badge,
                    row["user_name"],
                    row["product_name"],
                    row["quantity"],
                    format_price(row["total_price"]),
                    row["created_at"],
                    row["delivered_at"] or "",
                ), tags=tags)
        self.orders_tree.restore_selection_by_first_value(order_selected_id)

        if not silent:
            self.set_status("Đã tải đơn hàng")


    def refresh_users(self, silent: bool = False) -> None:
        repo = self.current_repo()
        selected_id = self.users_tree.get_selected_first_value()

        self.users_tree.show_skeleton(8)
        self.users_tree.clear()

        rows = repo.users(self.user_search_var.get())
        if not rows:
            self.users_tree.set_empty("Không có user", "User sẽ xuất hiện sau khi tương tác với bot")
        else:
            for row in rows:
                self.users_tree.add((
                    row["telegram_id"],
                    f"@{row['username']}" if row["username"] else "",
                    row["full_name"] or "",
                    format_price(row["balance"]),
                    row["created_at"],
                ))

        self.users_tree.restore_selection_by_first_value(selected_id)

        if not silent:
            self.set_status("Đã tải users")

    def build_stock_tab(self) -> None:
        top = ttk.Frame(self.tab_stock, style="Panel.TFrame")
        top.pack(fill="x", pady=(0, 10))

        self.stock_product_id_var = tk.StringVar()

        ttk.Label(top, text="Product ID", style="FieldLabel.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Entry(top, textvariable=self.stock_product_id_var, width=12).grid(row=0, column=1, sticky="w", padx=6)
        ttk.Button(top, text="Xem kho", command=self.load_stock).grid(row=0, column=2, padx=4)

        stock_menu_btn = ttk.Menubutton(top, text="⋯ Thao tác kho")
        stock_menu = tk.Menu(stock_menu_btn, tearoff=0)
        stock_menu.add_command(label="Xóa dòng đã chọn", command=self.delete_stock_line)
        stock_menu.add_command(label="Xóa toàn bộ kho chưa bán", command=self.clear_stock)
        stock_menu_btn["menu"] = stock_menu
        stock_menu_btn.grid(row=0, column=3, padx=4)

        ttk.Label(top, text="Tìm trong kho", style="FieldLabel.TLabel").grid(row=0, column=4, sticky="e", padx=(20, 6))
        stock_search_entry = ttk.Entry(top, textvariable=self.stock_search_var, width=30)
        stock_search_entry.grid(row=0, column=5, sticky="w")
        stock_search_entry.bind("<KeyRelease>", lambda _e: self.load_stock(silent=True))

        ttk.Label(top, text="Dán tài khoản, mỗi dòng 1 account", style="SectionTitle.TLabel").grid(
            row=1, column=0, columnspan=6, sticky="w", pady=(14, 6)
        )

        self.stock_input = tk.Text(
            top,
            height=10,
            wrap="word",
            bd=0,
            relief="flat",
            font=("Segoe UI", 10),
            bg=self.theme["input_bg"],
            fg=self.theme["text"],
            insertbackground=self.theme["text"],
        )
        self.stock_input.grid(row=2, column=0, columnspan=6, sticky="nsew")

        top.rowconfigure(2, weight=1)
        top.columnconfigure(5, weight=1)

        ttk.Button(top, text="Thêm vào kho", command=self.add_stock_action, style="Primary.TButton").grid(
            row=3, column=5, sticky="e", pady=10
        )

        box = ttk.Labelframe(self.tab_stock, text="Kho hiện tại", padding=10)
        box.pack(fill="both", expand=True)

        self.stock_tree = ModernTable(
            box,
            ("id", "product_id", "data"),
            {
                "id": "Stock ID",
                "product_id": "Product ID",
                "data": "Dữ liệu account",
            },
        )
        self.stock_tree.pack(fill="both", expand=True)

    def refresh_all(self) -> None:
        if not self.repo:
            return
        self.refresh_dashboard()
        self.refresh_categories(silent=True)
        self.refresh_products(silent=True)
        self.refresh_orders(silent=True)
        self.refresh_users(silent=True)
        self.load_settings()

    def refresh_categories(self, silent: bool = False) -> None:
        repo = self.current_repo()
        selected_id = self.categories_tree.get_selected_first_value()

        self.categories_tree.show_skeleton(6)
        self.categories_tree.clear()

        rows = repo.categories_with_counts(self.category_search_var.get())
        if not rows:
            self.categories_tree.set_empty("Không có danh mục", "Thử đổi từ khóa hoặc thêm danh mục mới")
        else:
            for row in rows:
                self.categories_tree.add((
                    row["id"],
                    row["emoji"] or "",
                    row["name"],
                    row["product_count"],
                    row["sort_order"],
                ))

        self.categories_tree.restore_selection_by_first_value(selected_id)

        if not silent:
            self.set_status("Đã tải danh mục")
        
    def refresh_categories(self, silent: bool = False) -> None:
        repo = self.current_repo()
        selected_id = self.categories_tree.get_selected_first_value()

        self.categories_tree.show_skeleton(6)
        self.categories_tree.clear()

        rows = repo.categories_with_counts(self.category_search_var.get())
        if not rows:
            self.categories_tree.set_empty("Không có danh mục", "Thử đổi từ khóa hoặc thêm danh mục mới")
        else:
            for row in rows:
                self.categories_tree.add((
                    row["id"],
                    row["emoji"] or "",
                    row["name"],
                    row["product_count"],
                    row["sort_order"],
                ))

        self.categories_tree.restore_selection_by_first_value(selected_id)

        if not silent:
            self.set_status("Đã tải danh mục")

    def refresh_orders(self, silent: bool = False) -> None:
        repo = self.current_repo()
        keyword = self.order_search_var.get()

        pending_selected_id = self.pending_tree.get_selected_first_value()
        order_selected_id = self.orders_tree.get_selected_first_value()

        self.pending_tree.show_skeleton(5)
        self.pending_tree.clear()
        pending_rows = repo.pending_orders(keyword)
        if not pending_rows:
            self.pending_tree.set_empty("Không có đơn chờ", "Mọi đơn chờ xác nhận sẽ hiển thị ở đây")
        else:
            for row in pending_rows:
                self.pending_tree.add((
                    row["id"],
                    "🔵 Mới",
                    row["user_name"],
                    row["product_name"],
                    row["quantity"],
                    format_price(row["total_price"]),
                    row["payment_code"],
                    row["created_at"],
                ), tags=("pending",))
        self.pending_tree.restore_selection_by_first_value(pending_selected_id)

        self.orders_tree.show_skeleton(8)
        self.orders_tree.clear()
        recent_rows = repo.recent_orders(keyword)
        if not recent_rows:
            self.orders_tree.set_empty("Chưa có đơn hàng", "Lịch sử đơn hàng sẽ hiển thị tại đây")
        else:
            for row in recent_rows:
                status = str(row["status"] or "").lower()
                tags = (status,) if status in {"pending", "delivered", "cancelled"} else ()
                badge = {
                    "pending": "🔵 Mới",
                    "delivered": "🟢 Đã giao",
                    "cancelled": "🔴 Hủy",
                }.get(status, status.title())
                self.orders_tree.add((
                    row["id"],
                    badge,
                    row["user_name"],
                    row["product_name"],
                    row["quantity"],
                    format_price(row["total_price"]),
                    row["created_at"],
                    row["delivered_at"] or "",
                ), tags=tags)
        self.orders_tree.restore_selection_by_first_value(order_selected_id)

        if not silent:
            self.set_status("Đã tải đơn hàng")

    def refresh_users(self, silent: bool = False) -> None:
        repo = self.current_repo()
        selected_id = self.users_tree.get_selected_first_value()

        self.users_tree.show_skeleton(8)
        self.users_tree.clear()

        rows = repo.users(self.user_search_var.get())
        if not rows:
            self.users_tree.set_empty("Không có user", "User sẽ xuất hiện sau khi tương tác với bot")
        else:
            for row in rows:
                self.users_tree.add((
                    row["telegram_id"],
                    f"@{row['username']}" if row["username"] else "",
                    row["full_name"] or "",
                    format_price(row["balance"]),
                    row["created_at"],
                ))

        self.users_tree.restore_selection_by_first_value(selected_id)

        if not silent:
            self.set_status("Đã tải users")

    def selected_category_id(self) -> int:
        self.focus_categories_tree()
        value = self.categories_tree.selected_value(0)
        if not value:
            raise AppError("Hãy chọn 1 danh mục trước")
        return int(value)

    def selected_product_id(self) -> int:
        self.focus_products_tree()
        value = self.products_tree.selected_value(0)
        if not value:
            raise AppError("Hãy chọn 1 sản phẩm trước")
        return int(value)

    def show_products_context_menu(self, event) -> None:
        try:
            item = self.products_tree.tree.identify_row(event.y)
            if not item:
                return

            self.products_tree.tree.selection_set(item)
            self.products_tree.tree.focus(item)

            self.products_context_menu.tk_popup(event.x_root, event.y_root)
        finally:
            try:
                self.products_context_menu.grab_release()
            except Exception:
                pass

    def selected_pending_order_id(self) -> int:
        value = self.pending_tree.selected_value(0)
        if not value:
            raise AppError("Hãy chọn 1 đơn chờ trước")
        return int(value)

    def selected_stock_id(self) -> int:
        value = self.stock_tree.selected_value(0)
        if not value:
            raise AppError("Hãy chọn 1 dòng stock trước")
        return int(value)

    def stock_product_id_value(self) -> int:
        text = self.stock_product_id_var.get().strip()
        if not text:
            raise AppError("Nhập Product ID trước")
        return int(text)

    def add_category_dialog(self) -> None:
        try:
            repo = self.current_repo()

            name = simpledialog.askstring("Thêm danh mục", "Tên danh mục:", parent=self)
            if name is None:
                return

            emoji = simpledialog.askstring("Thêm danh mục", "Emoji:", parent=self) or ""
            sort_order = simpledialog.askinteger("Thêm danh mục", "Sort order:", parent=self, initialvalue=99)
            if sort_order is None:
                return

            category_id = repo.add_category(name, emoji, sort_order)
            self.refresh_categories(silent=True)
            self.refresh_products(silent=True)
            self.set_status(f"Đã thêm danh mục #{category_id}")
            self.show_toast(f"Đã thêm danh mục #{category_id}", "success")
        except Exception as exc:
            messagebox.showerror("Lỗi", str(exc), parent=self)
            self.show_toast(str(exc), "error")

    def edit_category_dialog(self) -> None:
        try:
            self.focus_categories_tree()
            repo = self.current_repo()
            category_id = self.selected_category_id()

            row = repo.category(category_id)
            if not row:
                raise AppError("Không tìm thấy danh mục")

            name = simpledialog.askstring(
                "Sửa danh mục",
                "Tên mới:",
                parent=self,
                initialvalue=str(row["name"] or ""),
            )
            if name is None:
                return

            emoji = simpledialog.askstring(
                "Sửa danh mục",
                "Emoji:",
                parent=self,
                initialvalue=str(row["emoji"] or ""),
            )
            if emoji is None:
                return

            sort_order = simpledialog.askinteger(
                "Sửa danh mục",
                "Sort order:",
                parent=self,
                initialvalue=int(row["sort_order"] or 99),
            )
            if sort_order is None:
                return

            repo.update_category(category_id, name, emoji, sort_order)
            self.refresh_categories(silent=True)
            self.refresh_products(silent=True)
            self.set_status(f"Đã cập nhật danh mục #{category_id}")
            self.show_toast(f"Đã cập nhật danh mục #{category_id}", "success")
        except Exception as exc:
            messagebox.showerror("Lỗi", str(exc), parent=self)
            self.show_toast(str(exc), "error")

    def delete_category_dialog(self) -> None:
        try:
            repo = self.current_repo()
            category_id = self.selected_category_id()

            row = repo.category(category_id)
            if not row:
                raise AppError("Không tìm thấy danh mục")

            counts = repo.categories_with_counts()
            current = next((r for r in counts if int(r["id"]) == category_id), None)
            product_count = int(current["product_count"] or 0) if current else 0

            move_to = None
            if product_count > 0:
                move_to = simpledialog.askinteger(
                    "Xóa danh mục",
                    f"Danh mục #{category_id} đang có {product_count} sản phẩm.\nNhập ID danh mục đích để chuyển sản phẩm sang:",
                    parent=self,
                )
                if move_to is None:
                    return

            ok = messagebox.askyesno(
                "Xóa danh mục",
                f"Xóa danh mục #{category_id} - {row['name']}?",
                parent=self,
            )
            if not ok:
                return

            repo.delete_category(category_id, move_to)
            self.refresh_categories(silent=True)
            self.refresh_products(silent=True)
            self.set_status(f"Đã xóa danh mục #{category_id}")
            self.show_toast(f"Đã xóa danh mục #{category_id}", "success")
        except Exception as exc:
            messagebox.showerror("Lỗi", str(exc), parent=self)
            self.show_toast(str(exc), "error")

    def add_product_dialog(self) -> None:
        try:
            repo = self.current_repo()
            dlg = ProductDrawer(self, repo, theme=self.theme)
            self.wait_window(dlg)
            if dlg.result:
                product_id = repo.add_product(dlg.result)
                self.refresh_products(silent=True)
                self.refresh_dashboard()
                self.set_status(f"Đã thêm sản phẩm #{product_id}")
                self.show_toast(f"Đã thêm sản phẩm #{product_id}", "success")
        except Exception as exc:
            messagebox.showerror("Lỗi", str(exc), parent=self)
            self.show_toast(str(exc), "error")

    def edit_product_dialog(self) -> None:
        try:
            self.focus_products_tree()
            repo = self.current_repo()
            row = repo.product(self.selected_product_id())
            if not row:
                raise AppError("Không tìm thấy sản phẩm")

            dlg = ProductDrawer(self, repo, row, theme=self.theme)
            self.wait_window(dlg)

            if dlg.result:
                repo.update_product(int(row["id"]), dlg.result)
                self.refresh_products(silent=True)
                self.refresh_dashboard()
                self.set_status(f"Đã cập nhật sản phẩm #{row['id']}")
                self.show_toast(f"Đã cập nhật sản phẩm #{row['id']}", "success")
        except Exception as exc:
            messagebox.showerror("Lỗi", str(exc), parent=self)
            self.show_toast(str(exc), "error")

    def quick_edit_name(self) -> None:
        try:
            self.focus_products_tree()
            repo = self.current_repo()
            product_id = self.selected_product_id()

            row = repo.product(product_id)
            if not row:
                raise AppError("Không tìm thấy sản phẩm")

            value = simpledialog.askstring(
                "Đổi tên",
                f"Tên mới cho sản phẩm #{product_id}:",
                parent=self,
                initialvalue=str(row["name"] or ""),
            )
            if value is None:
                return

            repo.edit_name(product_id, value)
            self.refresh_products(silent=True)
            self.set_status(f"Đã đổi tên sản phẩm #{product_id}")
        except Exception as exc:
            messagebox.showerror("Lỗi", str(exc), parent=self)

    def quick_edit_price(self) -> None:
        try:
            repo = self.current_repo()
            product_id = self.selected_product_id()

            row = repo.product(product_id)
            if not row:
                raise AppError("Không tìm thấy sản phẩm")

            value = simpledialog.askinteger(
                "Đổi giá",
                f"Giá mới cho sản phẩm #{product_id}:",
                parent=self,
                initialvalue=int(row["price"] or 0),
                minvalue=0,
            )
            if value is None:
                return

            repo.edit_price(product_id, value)
            self.refresh_products(silent=True)
            self.refresh_dashboard()
            self.set_status(f"Đã cập nhật giá sản phẩm #{product_id}")
        except Exception as exc:
            messagebox.showerror("Lỗi", str(exc), parent=self)

    def toggle_product(self) -> None:
        try:
            repo = self.current_repo()
            product_id = self.selected_product_id()
            state = repo.toggle_product(product_id)

            self.refresh_products(silent=True)
            self.refresh_dashboard()
            self.set_status(f"Sản phẩm #{product_id} {'đã bật' if state else 'đã tắt'}")
            self.show_toast(f"Sản phẩm #{product_id} {'đã bật' if state else 'đã tắt'}", "success")
        except Exception as exc:
            messagebox.showerror("Lỗi", str(exc), parent=self)
            self.show_toast(str(exc), "error")

    def delete_product(self) -> None:
        try:
            repo = self.current_repo()
            product_id = self.selected_product_id()

            row = repo.product(product_id)
            if not row:
                raise AppError("Không tìm thấy sản phẩm")

            ok = messagebox.askyesno(
                "Xác nhận xóa",
                f"Xóa sản phẩm #{product_id} - {row['name']}?\nKho chưa bán cũng sẽ bị xóa.",
                parent=self,
            )
            if not ok:
                return

            repo.delete_product(product_id)
            self.refresh_products(silent=True)
            self.refresh_dashboard()
            self.set_status(f"Đã xóa sản phẩm #{product_id}")
            self.show_toast(f"Đã xóa sản phẩm #{product_id}", "success")

        except Exception as exc:
            msg = str(exc)
            if "Không thể xóa sản phẩm đã phát sinh đơn hàng" in msg:
                choose_disable = messagebox.askyesno(
                    "Không thể xóa",
                    "Sản phẩm này đã phát sinh đơn hàng nên không thể xóa.\n\nBạn có muốn chuyển sang TẮT sản phẩm ngay bây giờ không?",
                    parent=self,
                )
                if choose_disable:
                    try:
                        state = repo.toggle_product(product_id)
                        self.refresh_products(silent=True)
                        self.refresh_dashboard()
                        self.set_status(f"Sản phẩm #{product_id} {'đã bật' if state else 'đã tắt'}")
                        self.show_toast(f"Đã tắt sản phẩm #{product_id}", "warning" if not state else "success")
                    except Exception as inner_exc:
                        messagebox.showerror("Lỗi", str(inner_exc), parent=self)
                        self.show_toast(str(inner_exc), "error")
                return

            messagebox.showerror("Lỗi", msg, parent=self)
            self.show_toast(msg, "error")

    def load_stock(self, silent: bool = False) -> None:
        try:
            repo = self.current_repo()
            product_id = self.stock_product_id_value()

            row = repo.product(product_id)
            if not row:
                raise AppError("Sản phẩm không tồn tại")

            selected_stock_id = self.stock_tree.get_selected_first_value()

            self.stock_tree.show_skeleton(6)
            self.stock_tree.clear()

            items = repo.stock_items(product_id, self.stock_search_var.get())
            if not items:
                self.stock_tree.set_empty("Kho trống", "Chưa có dòng stock nào cho sản phẩm này")
            else:
                for item in items:
                    self.stock_tree.add((item["id"], item["product_id"], item["data"]))

            self.stock_tree.restore_selection_by_first_value(selected_stock_id)

            if not silent:
                self.set_status(f"Đã tải kho sản phẩm #{product_id}: {row['name']}")
        except Exception as exc:
            if not silent:
                messagebox.showerror("Lỗi", str(exc), parent=self)

    def add_stock_action(self) -> None:
        try:
            repo = self.current_repo()
            product_id = self.stock_product_id_value()

            count = repo.add_stock(product_id, self.stock_input.get("1.0", "end").splitlines())
            self.stock_input.delete("1.0", "end")

            self.load_stock(silent=True)
            self.refresh_products(silent=True)
            self.refresh_dashboard()
            self.set_status(f"Đã thêm {count} dòng vào kho của sản phẩm #{product_id}")
            self.show_toast(f"Đã thêm {count} dòng stock", "success")
        except Exception as exc:
            messagebox.showerror("Lỗi", str(exc), parent=self)
            self.show_toast(str(exc), "error")

    def delete_stock_line(self) -> None:
        try:
            repo = self.current_repo()
            stock_id = self.selected_stock_id()

            row = repo.stock_item(stock_id)
            if not row:
                raise AppError("Không tìm thấy dòng stock")

            ok = messagebox.askyesno(
                "Xóa dòng stock",
                f"Xóa stock #{stock_id}?\n\n{row['data']}",
                parent=self,
            )
            if not ok:
                return

            product_id = repo.delete_stock_item(stock_id)
            self.stock_product_id_var.set(str(product_id))

            self.load_stock(silent=True)
            self.refresh_products(silent=True)
            self.refresh_dashboard()
            self.set_status(f"Đã xóa dòng stock #{stock_id}")
            self.show_toast(f"Đã xóa stock #{stock_id}", "success")
        except Exception as exc:
            messagebox.showerror("Lỗi", str(exc), parent=self)
            self.show_toast(str(exc), "error")

    def clear_stock(self) -> None:
        try:
            repo = self.current_repo()
            product_id = self.stock_product_id_value()

            ok = messagebox.askyesno(
                "Xóa kho",
                f"Xóa toàn bộ kho chưa bán của sản phẩm #{product_id}?",
                parent=self,
            )
            if not ok:
                return

            count = repo.clear_stock(product_id)
            self.load_stock(silent=True)
            self.refresh_products(silent=True)
            self.refresh_dashboard()
            self.set_status(f"Đã xóa {count} dòng kho khỏi sản phẩm #{product_id}")
            self.show_toast(f"Đã xóa {count} dòng kho", "success")
        except Exception as exc:
            messagebox.showerror("Lỗi", str(exc), parent=self)
            self.show_toast(str(exc), "error")

    def confirm_auto(self) -> None:
        try:
            repo = self.current_repo()
            order_id = self.selected_pending_order_id()

            if not messagebox.askyesno(
                "Xác nhận tự động",
                f"Xác nhận đơn #{order_id} và giao bằng kho thật?",
                parent=self,
            ):
                return

            self.set_status("Đang xác nhận đơn tự động...")

            def task():
                result = repo.confirm_order_auto(order_id)
                repo.send_delivery_messages(result["order"], result["accounts"])
                return result

            def done(result):
                self.refresh_orders(silent=True)
                self.refresh_products(silent=True)
                self.refresh_dashboard()

                stock_pid = self.stock_product_id_var.get().strip()
                if stock_pid == str(result["order"]["product_id"]):
                    self.load_stock(silent=True)

                messagebox.showinfo(
                    "Thành công",
                    f"Đã xác nhận đơn #{order_id} và gửi {len(result['accounts'])} account cho khách.",
                    parent=self,
                )
                self.show_toast(f"Đơn #{order_id} đã giao thành công", "success")

            self.run_bg("Xác nhận tự động", task, done)
        except Exception as exc:
            messagebox.showerror("Lỗi", str(exc), parent=self)
            self.show_toast(str(exc), "error")

    def confirm_manual(self) -> None:
        try:
            repo = self.current_repo()
            order_id = self.selected_pending_order_id()
            row = repo.order(order_id)

            if not row:
                raise AppError("Không tìm thấy đơn")

            win = tk.Toplevel(self)
            win.title(f"Giao tay đơn #{order_id}")
            win.geometry("760x460")
            win.transient(self)
            win.grab_set()
            win.configure(bg=self.theme["bg"])

            shell = ttk.Frame(win, padding=14, style="Panel.TFrame")
            shell.pack(fill="both", expand=True)

            ttk.Label(
                shell,
                text=f"Giao tay đơn #{order_id}",
                style="DrawerTitle.TLabel",
            ).pack(anchor="w")
            ttk.Label(
                shell,
                text=f"{row['product_name']} × {row['quantity']} — dán dữ liệu, mỗi dòng 1 account",
                style="DrawerDesc.TLabel",
            ).pack(anchor="w", pady=(4, 10))

            text = tk.Text(
                shell,
                wrap="word",
                bd=0,
                relief="flat",
                font=("Segoe UI", 10),
                bg=self.theme["input_bg"],
                fg=self.theme["text"],
                insertbackground=self.theme["text"],
            )
            text.pack(fill="both", expand=True)

            btns = ttk.Frame(shell, style="Panel.TFrame")
            btns.pack(fill="x", pady=(10, 0))

            submit_btn = LoadingButton(btns, text="Giao tay", style="Primary.TButton")
            submit_btn.pack(side="right")
            ttk.Button(btns, text="Hủy", command=win.destroy).pack(side="right", padx=(0, 6))

            def submit() -> None:
                accounts = text.get("1.0", "end").splitlines()
                submit_btn.set_loading(True)

                win.destroy()
                self.set_status("Đang giao tay...")

                def task():
                    result = repo.confirm_order_manual(order_id, accounts)
                    repo.send_delivery_messages(result["order"], result["accounts"])
                    return result

                def done(result):
                    self.refresh_orders(silent=True)
                    self.refresh_products(silent=True)
                    self.refresh_dashboard()
                    messagebox.showinfo(
                        "Thành công",
                        f"Đã giao tay đơn #{order_id} và gửi {len(result['accounts'])} dòng cho khách.",
                        parent=self,
                    )
                    self.show_toast(f"Đã giao tay đơn #{order_id}", "success")

                self.run_bg("Giao tay", task, done)

            submit_btn.configure(command=submit)
        except Exception as exc:
            messagebox.showerror("Lỗi", str(exc), parent=self)
            self.show_toast(str(exc), "error")

    def cancel_order(self) -> None:
        try:
            repo = self.current_repo()
            order_id = self.selected_pending_order_id()

            if not messagebox.askyesno("Hủy đơn", f"Hủy đơn #{order_id}?", parent=self):
                return

            repo.cancel_order(order_id)
            self.refresh_orders(silent=True)
            self.refresh_dashboard()
            self.set_status(f"Đã hủy đơn #{order_id}")
            self.show_toast(f"Đơn #{order_id} đã bị hủy", "warning")
        except Exception as exc:
            messagebox.showerror("Lỗi", str(exc), parent=self)
            self.show_toast(str(exc), "error")

    def send_broadcast(self) -> None:
        try:
            repo = self.current_repo()
            text = self.broadcast_text.get("1.0", "end").strip()
            if not text:
                raise AppError("Bạn chưa nhập nội dung broadcast")

            if not messagebox.askyesno(
                "Broadcast",
                "Gửi thông báo này tới toàn bộ users?",
                parent=self,
            ):
                return

            self.broadcast_btn.set_loading(True, "Đang gửi...")
            self.set_status("Đang gửi broadcast...")

            def done(result):
                self.broadcast_btn.set_loading(False)
                messagebox.showinfo(
                    "Broadcast xong",
                    f"Tổng: {result['total']}\nThành công: {result['sent']}\nThất bại: {result['failed']}",
                    parent=self,
                )
                self.show_toast("Đã gửi thông báo Broadcast thành công", "success")

            def task():
                return repo.broadcast(text)

            self.run_bg("Broadcast", task, done)
        except Exception as exc:
            try:
                self.broadcast_btn.set_loading(False)
            except Exception:
                pass
            messagebox.showerror("Lỗi", str(exc), parent=self)
            self.show_toast(str(exc), "error")

    def load_settings(self) -> None:
        repo = self.current_repo()
        data = repo.config()
        for key, var in self.settings_vars.items():
            var.set(data.get(key, ""))

    def save_settings(self) -> None:
        try:
            repo = self.current_repo()
            repo.save_config({key: var.get().strip() for key, var in self.settings_vars.items()})

            auto_ms = repo.env.get("AUTO_REFRESH_MS", "").strip()
            if auto_ms.isdigit():
                self.auto_refresh_ms = max(2000, int(auto_ms))

            self.set_status("Đã lưu file .env")
            messagebox.showinfo("Xong", "Đã lưu cấu hình vào .env", parent=self)
            self.show_toast("Đã lưu cấu hình .env", "success")
        except Exception as exc:
            messagebox.showerror("Lỗi", str(exc), parent=self)
            self.show_toast(str(exc), "error")

def main() -> None:
    app = App()
    app.mainloop()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)