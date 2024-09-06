FROM python:3.12.5
ENV PYTHONUNBUFFERED=1
WORKDIR /waiter
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY waiter.py .