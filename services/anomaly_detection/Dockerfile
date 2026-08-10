FROM python:3.11-slim

WORKDIR /app

COPY services/anomaly_detection/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY services/common /app/services/common
COPY services/anomaly_detection /app/services/anomaly_detection

WORKDIR /app/services/anomaly_detection
ENV PYTHONUNBUFFERED=1

EXPOSE 9102
CMD ["python", "main.py"]
