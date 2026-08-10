FROM python:3.11-slim

WORKDIR /app

COPY simulator/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY services/common /app/services/common
COPY simulator /app/simulator

WORKDIR /app/simulator
ENV PYTHONUNBUFFERED=1

CMD ["python", "main.py"]
