# This is the config file for the Kafka Producer and Consumer
# 19.11.2020 - Rohan Kumar


# Topic definition
global_topic = 'write topic name here'
local_topic = 'Streaming'

# Kafka Producer definition
producer_local = {'bootstrap.servers': 'localhost:9092', 'message.max.bytes': 11957258, 'socket.keepalive.enable': True}

consumer_sarima_local = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'SARIMA',
    "socket.keepalive.enable": True,
    # "debug": "generic,broker,security"
    # 'auto.offset.reset': 'latest'
}

consumer_var_local = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'VAR',
    "socket.keepalive.enable": True,
    # "debug": "generic,broker,security"
    # 'auto.offset.reset': 'latest'
}
consumer_vecm_local = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'VECM',
    "socket.keepalive.enable": True,
    # "debug": "generic,broker,security"
    # 'auto.offset.reset': 'latest'
}

producer_global = {
    'bootstrap.servers': '#',
    'reconnect.backoff.ms': 100,
    'reconnect.backoff.max.ms': 1000,
    'compression.codec': 'gzip',
    'compression.type': 'gzip',
    'compression.level': 5,
    'message.max.bytes': 157286400,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'SCRAM-SHA-256',
    'sasl.username': "username",
    'sasl.password': "password",
    'socket.keepalive.enable': True,
    # 'debug': "generic,broker,security",
}

# Kafka Consumer definition
consumer_global = {

    'bootstrap.servers': '#',
    'group.id': 'SARIMA',
    'reconnect.backoff.ms': 100,
    'reconnect.backoff.max.ms': 1000,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'SCRAM-SHA-256',
    'sasl.username': "username",
    'sasl.password': "password",
    "socket.keepalive.enable": True,
}
