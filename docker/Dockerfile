FROM python:3.14-slim

RUN useradd -m  appuser

COPY dist/*.whl /tmp/
RUN pip install --no-cache-dir /tmp/kafka_republisher-*-py3-none-any.whl && rm /tmp/*.whl

ENV BOOTSTRAP_SERVERS=localhost:9092 \
    FROM_TOPIC=topicA \
    TO_TOPIC=topicB \
    SLEEP_TIME=30 \
    GROUP_ID=delayer

USER appuser
CMD ["kafka-republisher"]
