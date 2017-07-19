'use strict';

const   WebSocket = require('ws'),
        kafka = require('kafka-node'),
        EventEmitter = require('events'),
        Msg   = require('./msg'),
        debug = require('debug')('kafka-ws-connector');

class WSKafkaConnector extends EventEmitter {

    constructor(kc, wsc, prod_c, consumer_c){
        super();

        if(!wsc.port){
           throw new ReferenceError('WebSocket Server port is empty');
        }

        this.kafka_config = kc;
        this.websocket_config = wsc;
        this.producer_config = prod_c;
        this.kcc = consumer_c;
    }

    start(){
        this.wss = new WebSocket.Server(this.websocket_config);

        this.wss
            .on('connection', (ws, req) => {
                ws.cnt = this.wss.clients.length;
                try {
                    ws.pause();
                    this.initProducer(ws);

                    ws.on('close', () => {
                        this.emit('ws-close', ws);
                        ws.shutDown();
                    });
                    ws.on('pong', () => {
                        try {
                            ws.isAlive = true;
                            debug(`Pong received from ws ${ws.cnt}`);
                        } catch (e) {
                            console.error(e);
                        }
                    }); //heartbeat
                    this.wsOnMsg(ws);
                }catch(e) {
                    ws.resume();
                    throw e;
                }

                this.emit('ws-connection', ws, req);
            })
            .on('error', e => this.emit('error', e))
            .on('listening', () => this.emit('wss-ready', this.wss));

        this.ping_interval = setInterval(function ping() {
            this.wss.clients.forEach(ws => {
                if (ws.isAlive === false) return ws.terminate();

                ws.isAlive = false;
                debug(`Pinging ws ${ws.cnt}`);
                ws.ping('', false, true);
            });
        }, 60000);
    }

    stop(){
        clearInterval(this.ping_interval);
        this.wss.close();
    }

    initProducer(ws){
        debug(`Init producer on: ${this.kafka_config.kafkaHost}`);

        if(this.kafka_config.no_zookeeper_client === undefined || this.kafka_config.no_zookeeper_client === false)
            throw Error('no_zookeeper_client should be set to "true" while we don\'t support connection through ZooKeeper');

        const client = new kafka.KafkaClient(this.kafka_config);

        ws.producer = new kafka.Producer(client, this.prod_c);

        ws.producer
            .on('ready', () => {
                WSKafkaConnector._resumeWsWhenKafkaReady(ws);
                debug('Producer is ready')
                this.emit('producer-ready', ws.producer);
            })
            .on('error', e => this.emit('producer-error', e));

        ws.mq = [];

        if(this.kafka_config.mq_interval){
            ws.mq_send_interval = setInterval(ws.sendMessages.bind(ws), this.kafka_config.mq_interval);
            ws.mq_limit = this.kafka_config.mq_limit || 1;
        }else{
            ws.mq_limit = 1;
        }
    }

    initConsumer(ws, subscription_msg){
        debug(`Init consumer on: ${getBrokerList()}`);

        const client = new kafka.KafkaClient(this.kafka_config);


        let topicsPayload = subscription_msg.topics.map(t => {
            return {topic: t.topic, offset: t.offset || 0}
        });

        const producer_config_copy = Object.assign({}, this.producer_config);
        //Set consumer group received from the payload
        producer_config_copy.groupId = subscription_msg.consumer_group || producer_config_copy.groupId;

        ws.consumer = new kafka.Consumer(
            client,
            topicsPayload,
            producer_config_copy);


        ws.consumer
            .on('ready', function () {
                this.emit('consumer-read', ws.consumer);
                debug('Consumer is ready');
            })
            .on('error', e => this.emit('error', e))
            .on('offsetOutOfRange', e => {
                this.emit('error', e)
            })
            .on('message', function (data) {
                try {
                    this.emit('consumer-message', data)
                    ws.send(data.value);
                } catch (e) {
                    this.emit('error', e)
                    throw e;
                }
            });
//            .on('disconnected', () => debug(`Consumer of ws ${ws.cnt} has disconnected`));
    }

    static _resumeWsWhenKafkaReady(ws){
        if(ws.producer.ready){
            ws.resume();
            debug(`Resuming ws ${ws.cnt}`);
        }
    }

    static edCallback(e, d){
        if(e)console.error(e);
        if(d)debug(d);
    }

    wsOnMsg(ws){
        ws.on('message', msg => {
            debug(msg);
            this.emit('message', msg);

            try {

                let m = Msg.fromJSON(msg);

                if (m.isCreateTopics())
                 {
// const test_topics = [
//     {'test1': [0,1,2,3]}
// ]
                    // ws.producer.createTopics(test_topics, (e,d) =>{
                    ws.producer.createTopics(m.payload, (e,d) =>{


                        ws.send("Topic created" + d);

                        if(e) console.error(e);
                        else
                            debug(`Created topics ${m.payload}`);
                    })

                }
                if (m.isInfo) { //Get info about kafka topics
                    // debug(ws.producer);
                    ws.producer.client.loadMetadataForTopics([], (err, res) => {
                        if(err) console.error(err);

                        if(res && res.length > 1){
                            m.payload = Object.keys(res[1].metadata).filter(n => !n.startsWith('__'));
                            ws.send(m.toString());
                            debug(m.toString());
                        }
                    });
                }
                else if (m.isSubscribe) { //Subscribe to specific kafka topics
                    this.initConsumer(ws, m.payload);
                    debug(`Subscribed to topics: ${m.payload}`);
                }
                else if (m.isNotification) {
                    ws.addToQueue(m);
                }
            }
            catch (e) {
                if (e instanceof SyntaxError) {
                    console.error(`Invalid JSON string '${msg}'`);
                } else {
                    this.emit('error', e);
                    throw e;
                }
            }
        });
    }
}

module.exports.WSKafkaConnector = WSKafkaConnector;

WebSocket.prototype.addToQueue = function(message){
    this.mq.push(message);

    if(this.mq.length > 0){
        if(this.mq.length >= this.mq_limit){
            this.sendMessages();
        }
    }
};

WebSocket.prototype.shutDown = function() {

    try{
        debug(`Shutting down ws & kafka ${this.cnt}`);

        if(this.mq_send_interval)
            clearInterval(this.mq_send_interval);

        if(this.consumer && this.consumer.ready){
            this.consumer.close(WSKafkaConnector.edCallback);
            this.consumer = null;

        }
        if(this.producer && this.producer.ready){
            this.producer.close(WSKafkaConnector.edCallback);
            this.producer = null;
        }
    }catch(e){
        console.error(e);
    }
};

WebSocket.prototype.sendMessages = function(){

    try {
        if (this.mq.length === 0 || !this.producer.ready)
            return;

        let map = {};

        this.mq.forEach(msg => {
            let t = (map[msg.device_id] = map[msg.device_id] || {topic: msg.device_id});
            (t.messages = t.messages || []).push(msg.toString());
        });

        this.mq.length = 0;

        // this.producer.client.refreshMetadata();
        this.producer.send(Object.values(map), edCallback);

    } catch (e) {
        debug(e)
    }
};
