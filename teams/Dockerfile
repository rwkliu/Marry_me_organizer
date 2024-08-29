FROM python:3.12
ENV PYTHONUNBUFFERED=1
WORKDIR /security
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY security_worker.py .
COPY worker.py .
CMD ["python", "security_worker.py"]