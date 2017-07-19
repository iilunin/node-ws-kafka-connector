'use strict'

const WSKafka = require('../ws-kafka').WSKafkaConnector,
    debug = require('debug')('ws-kafka-test'),
    cfg = require('./config');


function getBrokerList(){
    return process.env.KAFKA_MBR || cfg.MBR_LIST;
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
    port: 8085
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

let wsk = new WSKafka(
    kafka_config,
    websocket_config,
    consumer_config
    );

wsk.on('ws-connection', (ws, req) => debug('connection'));

try {
    wsk.start();
}catch(e){
    debug(e);
    wsk.stop();
}
