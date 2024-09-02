FROM python:3.12.5
ENV PYTHONUNBUFFERED=1
WORKDIR /security
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY security.py .
CMD ["python", "security.py"]