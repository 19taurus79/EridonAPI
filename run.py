import uvicorn
import os
import sys

# Добавляем текущую директорию в путь поиска модулей, чтобы Python видел пакет new_agri_bot_backend
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

if __name__ == "__main__":
    # Запуск сервера с автоматической перезагрузкой при изменении кода
    uvicorn.run("new_agri_bot_backend.main:app", host="127.0.0.1", port=8000, reload=True)
