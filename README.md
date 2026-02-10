# 🎯  SHREEPADAM NIKUNJ

![Python](https://img.shields.io/badge/Python-3.10%2B-blue)
![Telegram](https://img.shields.io/badge/Telegram-Bot-blue)
![MongoDB](https://img.shields.io/badge/Database-MongoDB-green)
![Render](https://img.shields.io/badge/Deploy-Render-purple)
![License](https://img.shields.io/badge/License-Educational-orange)
![Status](https://img.shields.io/badge/Status-Active-success)

A **powerful Telegram Quiz Bot** made for students ❤️  
Create quizzes, run them in **groups or private**, manage quizzes, export quizzes to an interactive **HTML quiz player**, and much more.

> ✅ This project is shared absolutely free for students so they can build & deploy their own quiz bot.

---

## demo bot 
*Test bot on Telegram*  

[![Telegram](https://img.shields.io/badge/Telegram-2CA5E0?style=for-the-badge&logo=telegram&logoColor=white)](https://t.me/Quizindiarhbot)

---

## ✨ Highlights / Features

✅ Create unlimited quizzes  
✅ Play quizzes in **private** and **groups**  
✅ Quiz deep-link support (`/start <quiz_id>`)  
✅ Timer per question  
✅ Negative marking  
✅ MongoDB database support  
✅ Quiz management panel commands  
✅ Import questions from formats + polls  
✅ Export quiz as modern interactive HTML test page  
✅ Can run on **Render Free Service**

---

## 🧾 Tech Stack

- **Python**
- **python-telegram-bot**
- **MongoDB (pymongo)**
- **Flask + Gunicorn** (for Render web binding)

---

## all commands 
```
create - Create a new quiz interactively  
myquizzes - View all your quizzes  
manage - Manage your quizzes (delete, export)  
edit - Edit quiz details and questions  
post - Share quiz card in the chat  
schedule - Schedule a quiz for groups  
take - Start a quiz in private chat  
finish - End your active private quiz  
fast - Set fast mode timer (group admins)  
slow - Set slow mode timer (group admins)  
set_password - Set web panel password  
cancel - Cancel the current operation  
set_promo - Set a promotional message  
addauth - Grant premium status (owner only)  
rmauth - Remove premium status (owner only)  
ban - Ban a user from the bot (owner only)  
unban - Unban a user (owner only)  
status - View bot statistics (owner only)  
backup - Create a database backup (owner only)  
restore - Restore from backup (owner only)
```
---

## 📂 Project Structure

```
.
├── main.py             # Telegram bot (main script)
├── app.py              # Flask app for gunicorn binding
├── helper.py           # Extra helpers / edit flow
├── format.html         # HTML template for Quiz Export
├── requirements.txt    # Dependencies
└── qr.jpg              # UPI QR for donation
```

---

## ✅ Requirements

- Python 3.10+ recommended
- MongoDB URI (Mongo Atlas recommended)
- Telegram Bot Token from @BotFather

---
## First fork repo 
then change **api id and api hash** in main.py (line 66 ,67) 
then change force subscribe channel id, force subscribe channel link , poll group id , owner id 
bot must be admin in that chats . 

## ⚙️ Installation (Local Setup)

### 1️⃣ Clone Repo
```bast
```

### 2️⃣ Install Requirements
```bash
pip install -r requirements.txt
```

---

## 🔐 Environment Variables

Set these in terminal OR in Render dashboard:

| Variable | Description |
|---------|-------------|
| `book`  | Telegram Bot Token |
| `mon`   | MongoDB connection URI |
| `ses1` to `ses5` | Optional Pyrogram session strings (Userbot pool) |

### Example (Linux / Mac / Render Shell)
```bash
export book="YOUR_BOT_TOKEN"
export mon="YOUR_MONGO_URI"
export ses1="SESSION_STRING_1"
```

---

## ▶️ Run the Bot (Local)

```bash
python3 main.py
```

---

## 🚀 Deploy on Render (Free Service)

### ✅ Steps

1. Go to https://render.com
2. Create **New Web Service**
3. Connect GitHub repo
4. Add **Environment Variables**
5. Set Build + Start commands below
6. set Environment Variables 
---

### ✅ Build Command
```bash
pip install -r requirements.txt
```

---

### ✅ Start Command (IMPORTANT)

Use exactly this:

```bash
gunicorn app:app & python3 main.py
```

✅ `gunicorn app:app` keeps Flask web service alive  
✅ `python3 main.py` runs the Telegram Bot  
✅ `&` runs both together (Render friendly)

### Environment vars ( important)
```
bot = bot token 
mongo = url
ses1 or ses2 etc = session strings (optional)
```
---

## 🧠 Notes for Students

- Never hardcode your bot token in public repos
- Use `.env` locally (recommended)
- On Render, always add secrets in **Environment Variables**

---

## 🤝 Contributing

Pull requests are welcome ✅  
If you improve the project, feel free to open PR and help students more.

---

## ❤️ Donate / Support

If this project helped you, support me so I can keep providing free tools for students 🙏

### 📌 Scan UPI QR:
<img src="qr.png" alt="Donate via UPI" width="260"/>

⭐ Don’t forget to **Star this repo** — it motivates a lot!

---

## 📜 License

This project is released for educational use.  
You are free to learn, modify, and deploy it for your own learning projects.

---

## 👤 Author

Made with ❤️ for students  
**Telegram:** ANUJ PANDEY
