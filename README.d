i didn't use IDE all were build in terminal
For build service you need run build.sh file it will give an output into out\ directory
After building open two terminals in one you need run` java -cp "out:/home/levon/Kafka/kafka_2.13-3.9.0/libs/*:/home/levon/Downloads/psql/postgresql-42.6.0.jar" Consumer`
and another one ` java -cp "out:/home/levon/Kafka/kafka_2.13-3.9.0/libs/*" Application`
Application simulates random coordintes Consumer tracks, prepares report and saves them inside database

