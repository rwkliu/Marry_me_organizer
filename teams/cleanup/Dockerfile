FROM python:3.12.5
ENV PYTHONUNBUFFERED=1
WORKDIR /cleanup
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY cleanup.py .
CMD ["python", "cleanup.py"]