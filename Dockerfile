FROM python:3.8-slim

WORKDIR /app
COPY requirements.txt .
COPY collect.py .

RUN pip install -r requirements.txt

CMD ["python", "./collect.py"]
