const Kafka = require("node-rdkafka");
// read the KAFKA Brokers and KAFKA_TOPIC values from the local file config.js
// read the KAFKA Brokers and KAFKA_TOPIC values from the local file config.js

let messageCounter = 1;
const produceMessage = function (message) {
    console.log(`Produce message ${message}`)
    producer.produce(topic, 0, new Buffer.from(message), messageCounter++);
}

// construct a Kafka Configuration object understood by the node-rdkafka library
// merge the configuration as defined in config.js with additional properties defined here
const kafkaConf = {
    'metadata.broker.list': ['moped-01.srvs.cloudkafka.com:9094', 'moped-02.srvs.cloudkafka.com:9094' , 'moped-03.srvs.cloudkafka.com:9094'],
    'security.protocol' : 'SASL_SSL',
    'sasl.mechanisms': 'SCRAM-SHA-256',
    'sasl.username': "4lcfy0wc",
    'sasl.password': "H-mklk1JKFBRdU0Pi54euNb96GCi7Xvm",
    "socket.keepalive.enable": true,
    "debug": "generic,broker,security"
};

const topic = "4lcfy0wc-default";

// create a Kafka Producer - connected to the KAFKA_BROKERS defined in config.js
const producer = new Kafka.Producer(kafkaConf);
prepareProducer(producer)
// initialize the connection of the Producer to the Kafka Cluster
producer.connect();



function prepareProducer(producer) {
    // event handler attached to the Kafka Producer to handle the ready event that is emitted when the Producer has connected sucessfully to the Kafka Cluster
    producer.on("ready", function (arg) {
        console.log(`Producer connection to Kafka Cluster is ready`)
        produceMessage("This is a message from local js")
    });

    producer.on("disconnected", function (arg) {
        process.exit();
    });

    producer.on('event.error', function (err) {
        console.error(err);
        process.exit(1);
    });
    // This event handler is triggered whenever the event.log event is emitted, which is quite often
    producer.on('event.log', function (log) {
        // uncomment the next line if you want to see a log message every step of the way
        //console.log(log);
    });
}