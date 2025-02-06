I didn't use IDE all were build in terminal
For build service you need run build.sh file it will give an output into out\ directory
After building open two terminals in one you need run` java -cp "out:/path/to/Kafka/libs/*:/path/to/psql/postgresql-42.6.0.jar" Consumer`
and another one ` java -cp "out:/path/to/kafka/libs/*" Application`
Application simulates random coordintes Consumer tracks, prepares report and saves them inside database
""" Dont forget change `/path/to` into your path durinc compilation and running
