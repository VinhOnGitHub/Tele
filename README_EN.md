<p align="center">
  <img src="https://img.icons8.com/fluency/96/telegram-app.png" alt="Telegram Shop Bot" width="96"/>
</p>

<h1 align="center">🤖 Telegram Shop Bot</h1>

<p align="center">
  <strong>Automated Telegram selling bot with VietQR payment integration</strong><br/>
  <em>Stock management, auto-delivery, Google Sheet sync & multi-bank support</em>
</p>

<p align="center">
  <a href="README.md">🇻🇳 Tiếng Việt</a> | <b>🇬🇧 English</b>
</p>

<p align="center">
  <a href="#-quick-start"><img src="https://img.shields.io/badge/Setup-3_minutes-brightgreen?style=for-the-badge" alt="Setup"/></a>
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

## ✨ Key Features

| Feature | Description |
|---------|-------------|
| 🛒 **Automated selling** | Customer selects product → pays → receives account automatically |
| 💳 **VietQR Payment** | Instant QR code generation, supports 40+ Vietnamese banks |
| 🏦 **Multi-bank** | Lets customers choose between 2 payment banks |
| 📦 **Stock management** | Add/view/clear inventory, auto-deliver accounts on confirmation |
| 📊 **Google Sheet Sync** | Auto-sync products from Google Sheet (every 5 minutes) |
| 🔧 **Admin Panel** | Manage orders, products, view revenue statistics |
| 📢 **Broadcast** | Send announcements to all customers |
| 🔄 **Manual delivery** | Admin provides account details for out-of-stock products |

## 🔄 Purchase Flow

```
Customer: /product → Select Item → Choose Qty → Choose Bank
                          ↓
                Bot generates VietQR → Customer scans & pays
                          ↓
              Admin clicks ✅ Confirm (or /confirm)
                          ↓
             Bot auto-delivers account to customer ✅
```

## ⚡ Quick Start

### Requirements
- [Node.js](https://nodejs.org/) v18 or higher
- Telegram Bot Token (from [@BotFather](https://t.me/BotFather))
- Vietnamese bank account (supports [VietQR](https://vietqr.io/))

### 1️⃣ Clone & install

```bash
git clone https://github.com/kentzu213/telegram-shop-bot.git
cd telegram-shop-bot
npm install
```

### 2️⃣ Create Telegram Bot

1. Open Telegram → search [@BotFather](https://t.me/BotFather) → send `/newbot`
2. Set bot name → receive **Bot Token**
3. Send `/myid` to [@userinfobot](https://t.me/userinfobot) → get your **Telegram ID**

### 3️⃣ Configure

```bash
cp .env.example .env
```

Edit `.env` with your info:

```env
# Token from @BotFather
BOT_TOKEN=1234567890:ABCdefGhIjKlMnOpQrS

# Your Telegram ID (admin)
ADMIN_ID=123456789

# Bank info (lookup BIN codes: https://www.vietqr.io/danh-sach-ngan-hang)
BANK_BIN=970422
BANK_ACCOUNT=1234567890
BANK_ACCOUNT_NAME=NGUYEN VAN A
BANK_NAME=MB

# Shop info
SHOP_NAME=My Shop
SUPPORT_CONTACT=@your_username
```

### 4️⃣ Run the bot

```bash
npm start
```

> 💡 Dev mode (auto-restart on file changes): `npm run dev`

## 📋 Commands

<details>
<summary><b>👤 Customer Commands</b></summary>

| Command | Description |
|---------|-------------|
| `/start` | 🔄 Start / Restart bot |
| `/menu` | 👤 Account info |
| `/product` | 📦 Product listing |
| `/nap [amount]` | 💰 Top up balance |
| `/checkpay` | 🔍 Check payment status |
| `/support` | 🆘 Get support |
| `/myid` | 🆔 Get your Telegram ID |

</details>

<details>
<summary><b>🔧 Admin Commands</b></summary>

| Command | Description |
|---------|-------------|
| `/admin` | 📊 Admin dashboard |
| **Products** | |
| `/listproduct` | List all products |
| `/addproduct catID \| name \| price` | Add new product |
| `/editprice ID price` | Edit price |
| `/editname ID name` | Edit name |
| `/toggleproduct ID` | Enable/disable product |
| `/deleteproduct ID` | Delete product |
| `/addcategory name \| emoji` | Add category |
| **Inventory** | |
| `/addstock ID` | Add accounts to stock |
| `/viewstock ID` | View product stock |
| `/clearstock ID` | Clear unsold stock |
| **Orders** | |
| `/confirm orderID` | ✅ Confirm & deliver |
| `/pending` | View pending orders |
| `/cancelorder orderID` | Cancel order |
| `/orders` | Recent orders |
| **Other** | |
| `/stats` | Detailed statistics |
| `/users` | User list |
| `/broadcast` | Message all users |
| `/sync` | Sync from Google Sheet |
| `/setbank` | View bank info |
| `/setshop` | View/edit shop info |

</details>

## 📦 Adding Stock

```bash
# Step 1: Send command with product ID
/addstock 1

# Step 2: Send account list (one per line)
email1@example.com|password1|extra_info1
email2@example.com|password2|extra_info2
```

## 📊 Google Sheet Sync (optional)

Auto-sync your product catalog from Google Sheets:

| Column | Content |
|--------|---------|
| A | ID |
| B | Product name |
| C | Price |
| D | Unit |
| E | Stock quantity |
| F | In stock (TRUE/FALSE) |
| G | Contact URL (Zalo, etc.) |
| H | Notes / Promotions |

**Setup:**
1. In Google Sheets: **File → Share → Publish to web → Publish**
2. Copy Sheet ID from URL: `docs.google.com/spreadsheets/d/[SHEET_ID]/edit`
3. Add to `.env`:
```env
GOOGLE_SHEET_ID=your_sheet_id
SHEET_SYNC_INTERVAL=5
```

## 🏦 Multi-Bank Support

Add a second bank in `.env` to let customers choose:

```env
BANK2_BIN=970436
BANK2_ACCOUNT=9876543210
BANK2_ACCOUNT_NAME=NGUYEN VAN A
BANK2_NAME=VCB
```

## 🛠 Tech Stack

| Technology | Purpose |
|-----------|---------|
| [Node.js](https://nodejs.org/) | JavaScript runtime |
| [Telegraf v4](https://github.com/telegraf/telegraf) | Telegram Bot Framework |
| [better-sqlite3](https://github.com/WiseLibs/better-sqlite3) | SQLite database |
| [VietQR API](https://vietqr.io/) | QR payment code generation |
| [nanoid](https://github.com/ai/nanoid) | Unique payment code generator |

## 📁 Project Structure

```
telegram-shop-bot/
├── .env.example          # Config template (copy → .env)
├── package.json          # Dependencies
├── LICENSE               # MIT License
├── data/                 # SQLite database (auto-created)
└── src/
    ├── bot.js            # 🚀 Entry point
    ├── config.js         # ⚙️ Environment config loader
    ├── database.js       # 💾 Schema & seed data
    ├── commands/          # 📋 User commands
    │   ├── start.js
    │   ├── menu.js
    │   ├── product.js
    │   ├── nap.js
    │   ├── checkpay.js
    │   ├── support.js
    │   └── myid.js
    ├── handlers/          # ⚡ Callbacks & admin handlers
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
    └── utils/             # 🎨 Keyboards & message templates
        ├── keyboard.js
        └── messages.js
```

## 📞 Contact & Support

Need help with setup, customization, or have questions?

| Channel | Contact |
|---------|---------|
| 💬 **Telegram** | [@kentng](https://t.me/kentng) |
| 📱 **Zalo Group** | [Join group chat](https://zalo.me/g/agaxxc699) |
| 🐛 **Bug Report** | [Open Issue](https://github.com/kentzu213/telegram-shop-bot/issues) |

## 🤝 Contributing

Pull requests are welcome! For major changes, please open an issue first.

## 📄 License

[MIT](LICENSE) © 2026 [kentzu213](https://github.com/kentzu213)

---

<p align="center">
  If this project helped you, please ⭐ <b>star</b> the repo!<br/>
  💬 Contact: <a href="https://t.me/kentng">@kentng</a> | 📱 <a href="https://zalo.me/g/agaxxc699">Zalo Group</a>
</p>
