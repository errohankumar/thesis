# Streaming-Real-Time-Using-Apache-Kafka
An extension of my thesis to produce streaming data to the client server and display the forecast using javascript. The forecasts are made on the time-series data
of German-Luxembourg spot market and the real time forecasts for the 24 hours ahead are published to the Kafka brokers. The forecasts are then displayed using
Express JS.

### Required Python installations:
1. Pandas
2. Numpy
3. Statsmodels
4. Selenium

### Required Web Driver
1. Gecko Driver

### Required Apache Kafka
1. Apache Kafka 

### Required Web applications
1. ChartJS
2. Socket.IO
3. Node JS
5. Express JS
6. npm package manager


### To spin up Kafka follow the below command. This also creates global producer.

1. ~$sh bin/zookeeper-server-start.sh config/zookeeper.properties
2. ~$sh bin/kafka-server-start.sh config/server.properties





### Create a local consumer topic by running :
- bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic <topic name>

### Create a local producer topic by running:
- sh bin/kafka-console-producer.sh --broker-list localhost:9092 --topic <topic name>




### First run the kafka producer script and then the kafka consumer to see the messages recieved. Type in the messages to see if the consumer receives what is sent from the producer!

1. Node JS and Express JS installation for the Web App:
2. Install node package manager (system wide)
3. Download the npm installer from https://nodejs.org/en/
4. After installing check for the version $ npm -v
5. To check if node.js is installed : $ node -v
6. Install node version manager (system wide)

### Commands to spin the javascript app
- $ curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.37.0/install.sh | bash
- Install express js 
- $ npm install -g express-generator
- $ cd myapp
- $ npm install
- $ DEBUG=myapp:* npm start
- Open http://localhost:3000/

Warning : Delete the ‘npm modules’ before starting the ‘DEBUG=myapp:* npm start’

