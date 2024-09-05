FROM python:3.12.5
ENV PYTHONUNBUFFERED=1
WORKDIR /security
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY security.py .
CMD ["python", "security.py", "--queue_name", "security", "--channels", "3", "--exchange", "high", "--queue_bindings", "[{\"priority\": \"high\", \"routing_key\": \"accident.security\"},{\"priority\": \"high\", \"routing_key\": \"brawl.security\"},{\"priority\": \"high\", \"routing_key\": \"not_on_list.security\"}]"]