<p align="center">
  <img src="https://img.icons8.com/fluency/96/telegram-app.png" alt="Telegram Shop Bot" width="96"/>
</p>

<h1 align="center">🤖 Telegram Shop Bot</h1>

<p align="center">
  <strong>Bot bán hàng tự động trên Telegram với thanh toán QR VietQR</strong><br/>
  <em>Auto-selling Telegram bot with VietQR payment, stock management & Google Sheet sync</em>
</p>

<p align="center">
  <b>🇻🇳 Tiếng Việt</b> | <a href="README_EN.md">🇬🇧 English</a>
</p>

<p align="center">
  <a href="#-cài-đặt-nhanh"><img src="https://img.shields.io/badge/Cài_đặt-3_phút-brightgreen?style=for-the-badge" alt="Setup"/></a>
  <a href="https://github.com/kentzu213/telegram-shop-bot/stargazers"><img src="https://img.shields.io/github/stars/kentzu213/telegram-shop-bot?style=for-the-badge&color=yellow" alt="Stars"/></a>
  <a href="https://github.com/kentzu213/telegram-shop-bot/blob/main/LICENSE"><img src="https://img.shields.io/github/license/kentzu213/telegram-shop-bot?style=for-the-badge&color=blue" alt="License"/></a>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Node.js-18+-339933?style=flat-square&logo=node.js&logoColor=white" alt="Node.js"/>
  <img src="https://img.shields.io/badge/Telegraf-4.x-229ED9?style=flat-square&logo=telegram&logoColor=white" alt="Telegraf"/>
  <img src="https://img.shields.io/badge/SQLite-3-003B57?style=flat-square&logo=sqlite&logoColor=white" alt="SQLite"/>
  <img src="https://img.shields.io/badge/VietQR-Payment-FF6B35?style=flat-square" alt="VietQR"/>
  <img src="https://img.shields.io/badge/Google_Sheets-Sync-34A853?style=flat-square&logo=googlesheets&logoColor=white" alt="Google Sheets"/>
</p>

---

## ✨ Tính năng nổi bật

| Tính năng | Mô tả |
|-----------|--------|
| 🛒 **Bán hàng tự động** | Khách chọn sản phẩm → thanh toán → nhận hàng tự động |
| 💳 **QR VietQR** | Tạo mã QR thanh toán tức thì, hỗ trợ 40+ ngân hàng VN |
| 🏦 **2 ngân hàng** | Cho phép khách chọn ngân hàng khi thanh toán |
| 📦 **Quản lý kho** | Thêm/xem/xóa kho hàng, giao tài khoản tự động |
| 📊 **Google Sheet Sync** | Đồng bộ sản phẩm từ Google Sheet (tự động mỗi 5 phút) |
| 🔧 **Admin Panel** | Quản lý đơn hàng, sản phẩm, thống kê doanh thu |
| 📢 **Broadcast** | Gửi thông báo tới tất cả khách hàng |
| 🔄 **Giao hàng thủ công** | Admin cung cấp tài khoản trực tiếp cho sản phẩm hết kho |

## 🔄 Flow mua hàng

```
Khách: /product → Chọn SP → Chọn SL → Chọn Bank
                        ↓
              Bot tạo QR VietQR → Khách quét mã
                        ↓
            Admin nhấn ✅ Xác nhận (hoặc /confirm)
                        ↓
           Bot tự động gửi tài khoản cho khách ✅
```

## ⚡ Cài đặt nhanh

### Yêu cầu
- [Node.js](https://nodejs.org/) v18 trở lên
- Telegram Bot Token (từ [@BotFather](https://t.me/BotFather))
- Tài khoản ngân hàng (hỗ trợ [VietQR](https://vietqr.io/))

### 1️⃣ Clone & cài đặt

```bash
git clone https://github.com/kentzu213/telegram-shop-bot.git
cd telegram-shop-bot
npm install
```

### 2️⃣ Tạo Bot Telegram

1. Mở Telegram → tìm [@BotFather](https://t.me/BotFather) → gửi `/newbot`
2. Đặt tên bot → nhận **Bot Token**
3. Gửi `/myid` cho [@userinfobot](https://t.me/userinfobot) → nhận **Telegram ID**

### 3️⃣ Cấu hình

```bash
cp .env.example .env
```

Mở `.env` và điền thông tin:

```env
# Token từ @BotFather
BOT_TOKEN=1234567890:ABCdefGhIjKlMnOpQrS

# Telegram ID của admin
ADMIN_ID=123456789

# Ngân hàng (tra mã BIN: https://www.vietqr.io/danh-sach-ngan-hang)
BANK_BIN=970422
BANK_ACCOUNT=1234567890
BANK_ACCOUNT_NAME=NGUYEN VAN A
BANK_NAME=MB

# Thông tin shop
SHOP_NAME=My Shop
SUPPORT_CONTACT=@your_username
```

### 4️⃣ Chạy bot

```bash
npm start
```

> 💡 Dev mode (auto-restart): `npm run dev`

## 📋 Danh sách lệnh

<details>
<summary><b>👤 Lệnh người dùng</b></summary>

| Lệnh | Mô tả |
|-------|--------|
| `/start` | 🔄 Bắt đầu / Khởi động lại |
| `/menu` | 👤 Thông tin tài khoản |
| `/product` | 📦 Danh sách sản phẩm |
| `/nap [số tiền]` | 💰 Nạp số dư |
| `/checkpay` | 🔍 Kiểm tra thanh toán |
| `/support` | 🆘 Hỗ trợ |
| `/myid` | 🆔 Lấy Telegram ID |

</details>

<details>
<summary><b>🔧 Lệnh Admin</b></summary>

| Lệnh | Mô tả |
|-------|--------|
| `/admin` | 📊 Admin panel tổng quan |
| **Sản phẩm** | |
| `/listproduct` | Xem tất cả sản phẩm |
| `/addproduct catID \| tên \| giá` | Thêm sản phẩm mới |
| `/editprice ID giá` | Sửa giá |
| `/editname ID tên` | Sửa tên |
| `/toggleproduct ID` | Bật/tắt sản phẩm |
| `/deleteproduct ID` | Xóa sản phẩm |
| `/addcategory tên \| emoji` | Thêm danh mục |
| **Kho hàng** | |
| `/addstock ID` | Thêm tài khoản vào kho |
| `/viewstock ID` | Xem kho sản phẩm |
| `/clearstock ID` | Xóa kho chưa bán |
| **Đơn hàng** | |
| `/confirm orderID` | ✅ Xác nhận & giao hàng |
| `/pending` | Xem đơn chờ |
| `/cancelorder orderID` | Hủy đơn |
| `/orders` | Đơn hàng gần đây |
| **Khác** | |
| `/stats` | Thống kê chi tiết |
| `/users` | Danh sách users |
| `/broadcast` | Gửi thông báo all users |
| `/sync` | Đồng bộ Google Sheet |
| `/setbank` | Xem thông tin ngân hàng |
| `/setshop` | Xem/sửa thông tin shop |

</details>

## 📦 Thêm hàng vào kho

```bash
# Bước 1: Gửi lệnh với product ID
/addstock 1

# Bước 2: Gửi danh sách tài khoản (mỗi dòng 1 cái)
email1@example.com|password1|extra_info1
email2@example.com|password2|extra_info2
```

## 📊 Google Sheet Sync (tùy chọn)

Đồng bộ danh sách sản phẩm tự động từ Google Sheet:

| Cột | Nội dung |
|-----|----------|
| A | ID |
| B | Tên sản phẩm |
| C | Giá bán |
| D | Đơn vị |
| E | Số lượng trong kho |
| F | Còn hàng (TRUE/FALSE) |
| G | Link liên hệ (Zalo...) |
| H | Ghi chú / Khuyến mãi |

**Cách setup:**
1. **File → Chia sẻ → Xuất bản lên web → Xuất bản**
2. Copy Sheet ID từ URL: `docs.google.com/spreadsheets/d/[SHEET_ID]/edit`
3. Thêm vào `.env`:
```env
GOOGLE_SHEET_ID=your_sheet_id
SHEET_SYNC_INTERVAL=5
```

## 🏦 Hỗ trợ 2 ngân hàng

Thêm ngân hàng thứ 2 vào `.env` để khách được chọn:

```env
BANK2_BIN=970436
BANK2_ACCOUNT=9876543210
BANK2_ACCOUNT_NAME=NGUYEN VAN A
BANK2_NAME=VCB
```

## 🛠 Tech Stack

| Công nghệ | Mô tả |
|-----------|--------|
| [Node.js](https://nodejs.org/) | Runtime JavaScript |
| [Telegraf v4](https://github.com/telegraf/telegraf) | Telegram Bot Framework |
| [better-sqlite3](https://github.com/WiseLibs/better-sqlite3) | SQLite database |
| [VietQR API](https://vietqr.io/) | Tạo mã QR thanh toán |
| [nanoid](https://github.com/ai/nanoid) | Tạo mã thanh toán unique |

## 📁 Cấu trúc dự án

```
telegram-shop-bot/
├── .env.example          # Mẫu cấu hình (copy → .env)
├── package.json          # Dependencies
├── LICENSE               # MIT License
├── data/                 # SQLite database (tự tạo)
└── src/
    ├── bot.js            # 🚀 Entry point
    ├── config.js         # ⚙️ Load cấu hình từ .env
    ├── database.js       # 💾 Schema + seed data
    ├── commands/          # 📋 Lệnh user
    │   ├── start.js
    │   ├── menu.js
    │   ├── product.js
    │   ├── nap.js
    │   ├── checkpay.js
    │   ├── support.js
    │   └── myid.js
    ├── handlers/          # ⚡ Xử lý callback & admin
    │   ├── adminActions.js
    │   ├── paymentConfirm.js
    │   ├── productSelect.js
    │   └── quantitySelect.js
    ├── services/          # 🔧 Business logic
    │   ├── orderService.js
    │   ├── paymentService.js
    │   ├── productService.js
    │   ├── sheetSync.js
    │   └── userService.js
    └── utils/             # 🎨 Keyboard & messages
        ├── keyboard.js
        └── messages.js
```

## 📞 Liên hệ & Hỗ trợ

Nếu cần hỗ trợ cài đặt, tùy chỉnh, hoặc có câu hỏi:

| Kênh | Liên hệ |
|------|---------|
| 💬 **Telegram** | [@kentng](https://t.me/kentng) |
| 📱 **Nhóm Zalo** | [Tham gia nhóm chat](https://zalo.me/g/agaxxc699) |
| 🐛 **Bug Report** | [Mở Issue](https://github.com/kentzu213/telegram-shop-bot/issues) |

## 🤝 Đóng góp

Pull requests luôn được chào đón! Với thay đổi lớn, vui lòng mở issue trước.

## 📄 License

[MIT](LICENSE) © 2026 [kentzu213](https://github.com/kentzu213)

---

<p align="center">
  Nếu project hữu ích, hãy ⭐ <b>star</b> repo nhé!<br/>
  💬 Liên hệ: <a href="https://t.me/kentng">@kentng</a> | 📱 <a href="https://zalo.me/g/agaxxc699">Nhóm Zalo</a>
</p>
