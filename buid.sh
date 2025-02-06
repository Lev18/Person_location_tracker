#!/bin/bash
javac -cp "/path/to/kafka/libs*:/path/to/psql/postgresql-42.6.0.jar" -d out src/main/java/Consumer.java
javac -cp "/path/to/kafka/libs/*" -d out src/main/java/Application.java
