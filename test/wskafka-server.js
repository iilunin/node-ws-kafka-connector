'use strict'

const WSKafka = require('../ws-kafka').WSKafkaConnector,
    debug = require('debug')('ws-kafka-test'),
    cfg = require('./config'),
    path = require('path');

let conf_module = {};


try {

    if (process.env.CONF_DIR) {
        const p = path.format({dir: process.env.CONF_DIR,  base: 'ws_kafka_config' })
        conf_module = require(p);
        console.log(`config loaded form ${p}`);
    }
}catch(e){
    console.log(`default config loaded`);
    function getBrokerList(){
        return process.env.KAFKA_MBR || cfg.MBR_LIST;
    }

    function getWebSocketPort(){
        return process.env.WSS_PORT || cfg.WSS_PORT;
    }

    conf_module.kafka_config = {
        //node-kafka options
        kafkaHost: getBrokerList(),
        clientId: 'test-kafka-client-2',
        connectTimeout: 1000,
        requestTimeout: 60000,
        autoConnect: true,
        //custom options
        no_zookeeper_client: true,
        mq_limit: 20000,
        mq_interval: 200 //if null, then messages published immediately
    };

    conf_module.websocket_config ={
        port: getWebSocketPort()
    };

    conf_module.producer_config = {
        requireAcks: 1,
        ackTimeoutMs: 100,
        partitionerType: 2
    };

    conf_module.consumer_config ={
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
    };
}


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


const wsk = new WSKafka(
    conf_module.kafka_config,
    conf_module.websocket_config,
    conf_module.producer_config,
    conf_module.consumer_config
    );

wsk.on('ws-connection', (ws, req) => debug('connection'))
    .on('ws-close', () => debug('ws-close'))
    .on('wss-ready', () => debug('wss-ready'))
    .on('producer-ready', () => debug('producer-ready'))
    .on('producer-error', (e) => console.log(`producer-error ${e}`))
    .on('consumer-ready', () => debug('consumer-ready'))
    .on('consumer-error', (e) => console.log(`consumer-error ${e}`))
    .on('consumer-message', () => {})
    .on('error', (e) => console.log(`error ${e}`));


process.on('uncaughtException', e => {
    debug(e);
    wsk.stop.bind(wsk);
    wsk.stop();
}).on('SIGINT', function exit(){
        debug("EXIT");
        wsk.stop();
    }
);

try {
    wsk.start();
}catch(e){
    debug(e);
    wsk.stop();
}

