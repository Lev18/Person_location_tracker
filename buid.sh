#!/bin/bash
javac -cp "/home/levon/Kafka/kafka_2.13-3.9.0/libs/*:/home/levon/Downloads/psql/postgresql-42.6.0.jar" -d out src/main/java/Consumer.java
javac -cp "/home/levon/Kafka/kafka_2.13-3.9.0/libs/*" -d out src/main/java/Application.java
