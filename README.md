# kafka-republisher

docker image to republish kafka message from one topic to another, optionally with delay

Example environments:

```
      BOOTSTRAP_SERVERS: kafka:9092
      FROM_TOPIC: topicA
      TO_TOPIC: topicB
      SLEEP_TIME: 30
      GROUP_ID: delayer
```

# docker build

```
cd docker
docker build -t siakhooi/kafka-republisher:latest .

docker login -p xxxtoken
docker push siakhooi/kafka-republisher:latest
```
