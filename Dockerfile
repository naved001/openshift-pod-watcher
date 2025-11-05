FROM python:3.13-slim

WORKDIR /app
COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY src/ /app/src
COPY bin/run.sh /app/run.sh

CMD ["./run.sh"]
