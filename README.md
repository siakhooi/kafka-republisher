# kafka-republisher

docker image to republish kafka message from one topic to another, optionally with delay

# docker build

```
cd docker
docker build -t siakhooi/kafka-republisher:latest .

docker login -p xxxtoken
docker push siakhooi/kafka-republisher:latest
```
