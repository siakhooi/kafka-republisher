# kafka-republisher

Republish kafka message from one topic to another, optionally with delay

Example environments:

```
      BOOTSTRAP_SERVERS: kafka:9092
      FROM_TOPIC: topicA
      TO_TOPIC: topicB
      SLEEP_TIME: 30
      GROUP_ID: delayer
```

## docker build

```
cd docker
docker build -t siakhooi/kafka-republisher:latest .

docker login -p xxxtoken
docker push siakhooi/kafka-republisher:latest
```

## Deliverables

- https://hub.docker.com/r/siakhooi/kafka-republisher

## Installation

```
pip install kafka_republisher
```

## Usage

```
$ kafka-republisher -h
usage: kafka-republisher [-h] [-v]

Kafka Mock Messages Sender

options:
  -h, --help     show this help message and exit
  -v, --version  show program's version number and exit

```

## Links

- https://pypi.org/project/kafka_republisher/
- https://github.com/siakhooi/kafka-republisher
- https://sonarcloud.io/project/overview?id=siakhooi_kafka-republisher
- https://qlty.sh/gh/siakhooi/projects/kafka-republisher

## Badges

![GitHub](https://img.shields.io/github/license/siakhooi/kafka-republisher?logo=github)
![GitHub last commit](https://img.shields.io/github/last-commit/siakhooi/kafka-republisher?logo=github)
![GitHub tag (latest by date)](https://img.shields.io/github/v/tag/siakhooi/kafka-republisher?logo=github)
![GitHub issues](https://img.shields.io/github/issues/siakhooi/kafka-republisher?logo=github)
![GitHub closed issues](https://img.shields.io/github/issues-closed/siakhooi/kafka-republisher?logo=github)
![GitHub pull requests](https://img.shields.io/github/issues-pr-raw/siakhooi/kafka-republisher?logo=github)
![GitHub closed pull requests](https://img.shields.io/github/issues-pr-closed-raw/siakhooi/kafka-republisher?logo=github)
![GitHub top language](https://img.shields.io/github/languages/top/siakhooi/kafka-republisher?logo=github)
![GitHub language count](https://img.shields.io/github/languages/count/siakhooi/kafka-republisher?logo=github)
![Lines of code](https://img.shields.io/tokei/lines/github/siakhooi/kafka-republisher?logo=github)
![GitHub repo size](https://img.shields.io/github/repo-size/siakhooi/kafka-republisher?logo=github)
![GitHub code size in bytes](https://img.shields.io/github/languages/code-size/siakhooi/kafka-republisher?logo=github)

![Workflow](https://img.shields.io/badge/Workflow-github-purple)
![workflow](https://github.com/siakhooi/kafka-republisher/actions/workflows/build.yaml/badge.svg)
![workflow](https://github.com/siakhooi/kafka-republisher/actions/workflows/workflow-deployments.yml/badge.svg)

![Release](https://img.shields.io/badge/Release-github-purple)
![GitHub release (latest by date)](https://img.shields.io/github/v/release/siakhooi/kafka-republisher?label=GPR%20release&logo=github)
![GitHub all releases](https://img.shields.io/github/downloads/siakhooi/kafka-republisher/total?color=33cb56&logo=github)
![GitHub Release Date](https://img.shields.io/github/release-date/siakhooi/kafka-republisher?logo=github)

![Quality-Qlty](https://img.shields.io/badge/Quality-Qlty-purple)
[![Maintainability](https://qlty.sh/gh/siakhooi/projects/kafka-republisher/maintainability.svg)](https://qlty.sh/gh/siakhooi/projects/kafka-republisher)
[![Code Coverage](https://qlty.sh/gh/siakhooi/projects/kafka-republisher/coverage.svg)](https://qlty.sh/gh/siakhooi/projects/kafka-republisher)

![Quality-Sonar](https://img.shields.io/badge/Quality-SonarCloud-purple)
[![Code Smells](https://sonarcloud.io/api/project_badges/measure?project=siakhooi_kafka-republisher&metric=code_smells)](https://sonarcloud.io/summary/new_code?id=siakhooi_kafka-republisher)
[![Duplicated Lines (%)](https://sonarcloud.io/api/project_badges/measure?project=siakhooi_kafka-republisher&metric=duplicated_lines_density)](https://sonarcloud.io/summary/new_code?id=siakhooi_kafka-republisher)
[![Bugs](https://sonarcloud.io/api/project_badges/measure?project=siakhooi_kafka-republisher&metric=bugs)](https://sonarcloud.io/summary/new_code?id=siakhooi_kafka-republisher)
[![Vulnerabilities](https://sonarcloud.io/api/project_badges/measure?project=siakhooi_kafka-republisher&metric=vulnerabilities)](https://sonarcloud.io/summary/new_code?id=siakhooi_kafka-republisher)
[![Technical Debt](https://sonarcloud.io/api/project_badges/measure?project=siakhooi_kafka-republisher&metric=sqale_index)](https://sonarcloud.io/summary/new_code?id=siakhooi_kafka-republisher)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=siakhooi_kafka-republisher&metric=sqale_rating)](https://sonarcloud.io/summary/new_code?id=siakhooi_kafka-republisher)
[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=siakhooi_kafka-republisher&metric=security_rating)](https://sonarcloud.io/summary/new_code?id=siakhooi_kafka-republisher)
[![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=siakhooi_kafka-republisher&metric=reliability_rating)](https://sonarcloud.io/summary/new_code?id=siakhooi_kafka-republisher)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=siakhooi_kafka-republisher&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=siakhooi_kafka-republisher)
[![Lines of Code](https://sonarcloud.io/api/project_badges/measure?project=siakhooi_kafka-republisher&metric=ncloc)](https://sonarcloud.io/summary/new_code?id=siakhooi_kafka-republisher)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=siakhooi_kafka-republisher&metric=coverage)](https://sonarcloud.io/summary/new_code?id=siakhooi_kafka-republisher)
![Sonar Violations (short format)](https://img.shields.io/sonar/violations/siakhooi_kafka-republisher?server=https%3A%2F%2Fsonarcloud.io)
![Sonar Violations (short format)](https://img.shields.io/sonar/blocker_violations/siakhooi_kafka-republisher?server=https%3A%2F%2Fsonarcloud.io)
![Sonar Violations (short format)](https://img.shields.io/sonar/critical_violations/siakhooi_kafka-republisher?server=https%3A%2F%2Fsonarcloud.io)
![Sonar Violations (short format)](https://img.shields.io/sonar/major_violations/siakhooi_kafka-republisher?server=https%3A%2F%2Fsonarcloud.io)
![Sonar Violations (short format)](https://img.shields.io/sonar/minor_violations/siakhooi_kafka-republisher?server=https%3A%2F%2Fsonarcloud.io)
![Sonar Violations (short format)](https://img.shields.io/sonar/info_violations/siakhooi_kafka-republisher?server=https%3A%2F%2Fsonarcloud.io)
![Sonar Violations (long format)](https://img.shields.io/sonar/violations/siakhooi_kafka-republisher?format=long&server=http%3A%2F%2Fsonarcloud.io)

[![Generic badge](https://img.shields.io/badge/Funding-BuyMeACoffee-33cb56.svg)](https://www.buymeacoffee.com/siakhooi)
[![Generic badge](https://img.shields.io/badge/Funding-Ko%20Fi-33cb56.svg)](https://ko-fi.com/siakhooi)

![visitors](https://hit-tztugwlsja-uc.a.run.app/?outputtype=badge&counter=ghmd-kafka-republisher)
