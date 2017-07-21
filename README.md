[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

# Web socket connector to Kafka

WebSocket server which implements message protocol to work with some basic kafka APIs,
such as create, list, subscribe to topics, push messages.

Based on [SOHU-Co/kafka-node](https://github.com/SOHU-Co/kafka-node) 
and [WebSockets](https://github.com/websockets/ws)

```javascript
const kafka_config = {
    //node-kafka options
    kafkaHost: 'localhost:9092',
    clientId: 'test-kafka-client-2',
    connectTimeout: 1000,
    requestTimeout: 60000,
    autoConnect: true,
    //custom options
    no_zookeeper_client: true,
    mq_limit: 20000,
    mq_interval: 1000 //if null, then messages published immediately
}

const websocket_config ={
    port: 8085
}

const producer_config = {
    requireAcks: 1,
    ackTimeoutMs: 100,
    partitionerType: 2
}

const consumer_config ={
    groupId: 'kafka-node-group', //should be set by message to ws
        // Auto commit config
    autoCommit: true,
    autoCommitMsgCount: 100,
    autoCommitIntervalMs: 5000,
    // Fetch message config
    fetchMaxWaitMs: 100,
    fetchMinBytes: 1,
    fetchMaxBytes: 1024 * 10,
}

const wsk = new WSKafka(
    kafka_config,
    websocket_config,
    producer_config,
    consumer_config
    );

wsk.start();

```