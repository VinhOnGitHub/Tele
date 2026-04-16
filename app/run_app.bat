@echo off
cd /d %~dp0
py app.py
pause@echo off
setlocal
cd /d %~dp0

py -m pip install --upgrade pip
py -m pip install -r requirements.txt
py -m PyInstaller --noconfirm --onefile --windowed --name TelegramShopAdmin app.py

echo.
echo Build xong. File EXE nam trong: dist\TelegramShopAdmin.exe
pause