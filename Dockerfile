# Dockerfile (расположен в папке api/)

FROM python:3.11-alpine3.17

# Устанавливаем зависимости для сборки, необходимые для некоторых Python-пакетов
RUN apk add --no-cache build-base python3-dev linux-headers

# Устанавливаем рабочую директорию внутри контейнера
WORKDIR /app

# Копируем файл зависимостей и другие конфигурационные файлы из папки 'api/'
# (Теперь папка 'api/' является контекстом сборки, поэтому просто указываем имя файла)
COPY requirements.txt /app/
COPY piccolo_conf.py /app/
COPY .env /app/

# Устанавливаем Python зависимости
RUN pip install --no-cache-dir --break-system-packages -r /app/requirements.txt

# Копируем весь остальной код приложения из текущего контекста (папки 'api/')
# Вся папка 'api/' (теперь включая new_agri_bot_backend/) копируется в /app
COPY . /app/

# Переменная окружения для Piccolo (если нужна в Dockerfile)
ENV PICCOLO_CONF="piccolo_conf"

# Открываем порт, на котором будет работать FastAPI
EXPOSE 8000

# Команда для запуска приложения с Uvicorn
# 'new_agri_bot_backend.main:app' указывает, что main.py находится в подпапке new_agri_bot_backend
CMD ["uvicorn", "new_agri_bot_backend.main:app", "--host", "0.0.0.0", "--port", "8000"]