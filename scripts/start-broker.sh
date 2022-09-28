cp conf.py conf &&. ./conf && rm conf
echo 'It should be running in another console. Kafka should be started before'
echo "/usr/bin/kafka-console-consumer --bootstrap-server $BROKER_URL --topic $TOPIC_NAME --from-beginning --max-messages 10"
echo

python ../kafka_server.py