FROM python:3.11-slim

WORKDIR /app

COPY services/monitoring/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY services/common /app/services/common
COPY services/monitoring /app/services/monitoring

WORKDIR /app/services/monitoring
ENV PYTHONUNBUFFERED=1

EXPOSE 9103
CMD ["python", "main.py"]
