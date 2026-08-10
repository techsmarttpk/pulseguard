FROM python:3.11-slim

WORKDIR /app

COPY services/ingestion/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY services/common /app/services/common
COPY services/ingestion /app/services/ingestion

WORKDIR /app/services/ingestion
ENV PYTHONUNBUFFERED=1

EXPOSE 9101
CMD ["python", "main.py"]
