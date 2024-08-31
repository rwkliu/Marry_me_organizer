FROM python:3.12
ENV PYTHONUNBUFFERED=1
WORKDIR /security
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY security_high_accident.py .
CMD ["python", "security_high_accident.py"]