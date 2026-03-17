FROM python:3.9-slim

# Устанавливаем системные зависимости для pymorphy2
RUN apt-get update && apt-get install -y \
    wget \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Копируем зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Скачиваем словари pymorphy2 отдельно (для надежности)
RUN python -c "import pymorphy2; pymorphy2.MorphAnalyzer(lang='ru')" || true

# Копируем весь проект
COPY . .

# Создаем необходимые папки
RUN mkdir -p exports backups logs data

CMD ["python", "main.py"]
