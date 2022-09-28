# bdt-project
*****TEST DATAS AND RESULTS WILL BE UPLOADED AS SOON AS POSSIBLE

To start Kafka
cd kafka
bin/zookeeper-server-start.sh config/zookeeper.properties 
 # run it in another terminal it runs kafka brokers
bin/kafka-server-start.sh config/server.properties
 # Create topic tweets, run in another terminal
bin/kafka-topics.sh --create --topic tweets --replication-factor 1 --partitions 1 --bootstrap-server localhost:9092

# To can see the topics
bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# install python3-pip
sudo apt-get install python3-pip

# install kafka-python
pip3 install kafka-python
pip3 list | grep kafka
