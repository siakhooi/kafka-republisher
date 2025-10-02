FROM python:3.13-slim

WORKDIR /app
COPY republisher.py /app/

RUN pip install confluent-kafka

ENV BOOTSTRAP_SERVERS=localhost:9092 \
    FROM_TOPIC=topicA \
    TO_TOPIC=topicB \
    SLEEP_TIME=30 \
    GROUP_ID=delayer

ENTRYPOINT ["python", "republisher.py"]
