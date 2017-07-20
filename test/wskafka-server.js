'use strict'

const WSKafka = require('../ws-kafka').WSKafkaConnector,
    debug = require('debug')('ws-kafka-test'),
    cfg = require('./config');


// Topics:
// create topic message example:
// {"id":23, "t":"topic","a":"create", "p":["t1", "t2", "t3", "t4"]}
//
// list topics:
// {"id":157, "t":"topic","a":"list"}
//
// subscribe to topic:
// {"id":1000,"t":"topic","a":"subscribe","p":{"t":["t1","t2"],"consumer_group":"ingestion_1"}}
//
// send notification. payload: t - topic, m - your message
// {"id":5,"t":"notif","a":"create","p":{"t":"t2", "m":"hello"}}



function getBrokerList(){
    return process.env.KAFKA_MBR || cfg.MBR_LIST;
}

function getWebSocketPort(){
    return process.env.WSS_PORT || cfg.WSS_PORT;
}

const kafka_config = {
    //node-kafka options
    kafkaHost: getBrokerList(),
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
    port: getWebSocketPort()
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
    fetchMaxBytes: 1024 * 1024,
    // Offset
    fromOffset: false
}

const wsk = new WSKafka(
    kafka_config,
    websocket_config,
    producer_config,
    consumer_config
    );

wsk.on('ws-connection', (ws, req) => debug('connection'))
    .on('ws-close', () => debug('ws-close'))
    .on('wss-ready', () => debug('wss-ready'))
    .on('producer-ready', () => debug('producer-ready'))
    .on('producer-error', () => debug('producer-error'))
    .on('consumer-ready', () => debug('consumer-ready'))
    .on('consumer-error', () => debug('consumer-error'))
    .on('consumer-message', () => debug('consumer-message'))
    .on('error', () => debug('error'));


process.on('uncaughtException', e => {
    debug(e);
    wsk.stop.bind(wsk);
    wsk.stop();
}).on('SIGINT', wsk.stop.bind(wsk));

try {
    wsk.start();
}catch(e){
    debug(e);
    wsk.stop();
}

