
# Python 3.12 для сервиса мониторинга ТПУ
FROM python:3.12-slim

# Настройки Python
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Устанавливаем зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем код проекта
COPY . .

# Создаём пользователя и каталог базы данных
RUN useradd --create-home --uid 10001 monitoring \
    && mkdir -p /data \
    && chown monitoring:monitoring /data

USER monitoring

# Порт FastAPI внутри контейнера
EXPOSE 8000

# Запуск основного API
CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
