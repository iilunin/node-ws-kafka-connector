'use strict';

const WebSocket = require('ws'),
    kafka = require('kafka-node'),
    EventEmitter = require('events'),
    Msg = require('./msg'),
    debug = require('debug')('kafka-ws-connector');

class WSKafkaConnector extends EventEmitter {

    constructor(kc, wsc, prod_c, consumer_c) {
        super();

        if (!wsc.port) {
            throw new ReferenceError('WebSocket Server port is empty');
        }

        this.kafka_config = kc;
        this.websocket_config = wsc;
        this.producer_config = prod_c;
        this.consumer_config = consumer_c;
    }

    start() {
        this.wss = new WebSocket.Server(this.websocket_config);

        this.wss
            .on('connection', (ws, req) => {
                ws.cnt = this.wss.clients.size;
                try {
                    ws.on('close', (code, reason) => {
                        debug(`closing web socket connection (code: ${code}, reason: "${reason}") ${ws.cnt}`);
                        this.emit('ws-close', ws);
                        ws.shutDown();
                    }).on('pong', () => {
                        try {
                            ws.isAlive = true;
                            debug(`Pong received from ws ${ws.cnt}`);
                        } catch (e) {
                            console.error(e);
                        }
                    }).on('message', msg => {
                        this.handleWebSocketMessage(msg, ws);
                    });

                    ws.pause();
                    this.initProducer(ws);

                } catch (e) {
                    ws.resume();
                    throw e;
                }

                this.emit('ws-connection', ws, req);
            })
            .on('error', e => this.raiseError(e))
            .on('listening', () => this.emit('wss-ready', this.wss));

        this.ping_interval = setInterval(function () {
            this.wss.clients.forEach(ws => {
                if (ws.isAlive === false) return ws.terminate();

                ws.isAlive = false;
                debug(`Pinging ws ${ws.cnt}`);
                ws.ping('', false, true);
            });
        }.bind(this), 30000);
    }

    handleWebSocketMessage(msg, ws) {
        debug(msg);
        this.emit('ws-message', msg);
        try {

            let m = Msg.fromJSON(msg);
            if (m.isTypeTopic) {
                this.handleTopicMessage(ws, m);
            }
            else if (m.isNotification) {
                ws.addToQueue(m);
            }
        }
        catch (e) {
            this.raiseError(e);
            if (e instanceof SyntaxError) {
                debug(`Invalid JSON string '${msg}'`);
            }

            debug(`error ${e}`)
        }
    }

    stop() {
        debug("stopping connector");
        clearInterval(this.ping_interval);
        if (this.wss)
            this.wss.close();
    }

    initProducer(ws) {
        debug(`Init producer on: ${this.kafka_config.kafkaHost}`);

        if (this.kafka_config.no_zookeeper_client === undefined || this.kafka_config.no_zookeeper_client === false)
            throw Error('no_zookeeper_client should be set to "true" while we don\'t support connection through ZooKeeper');

        const client = new kafka.KafkaClient(Object.assign({}, this.kafka_config));

        ws.producer = new kafka.Producer(client, this.producer_config);

        ws.producer
            .on('ready', () => {
                WSKafkaConnector._resumeWsWhenKafkaReady(ws);
                debug('Producer is ready');
                this.emit('producer-ready', ws.producer);
            })
            .on('error', e => this.raiseError('producer-error', e));

        ws.mq = [];

        if (this.kafka_config.mq_interval) {
            ws.mq_send_interval = setInterval(ws.sendMessages.bind(ws), this.kafka_config.mq_interval);
            ws.mq_limit = this.kafka_config.mq_limit || 1;
        } else {
            ws.mq_limit = 1;
        }
    }

    raiseError(e, et = 'error') {
        debug(e);
        this.emit(et, e);
    }

    static _resumeWsWhenKafkaReady(ws) {
        if (ws.producer.ready) {
            ws.resume();
            debug(`Resuming ws ${ws.cnt}`);
        }
    }

    static edCallback(e, d) {
        if (e) console.error(e);
        if (d) debug(d);
    }

    handleTopicMessage(ws, msg) {
        let rm = Msg.createReplyMessage(msg);
        rm.status = Msg.STATUS_FAIL;

        function setPayloadAndSend(p){
            rm.payload = p;
            ws.send(rm.toString());
        }

        switch (msg.action) {
            case Msg.ACTION_CREATE:
                if (!msg.hasPayload) {
                    setPayloadAndSend('specify topics list in the "payload"');
                } else {
                    ws.producer.createTopics(msg.payload, (e, d) => {
                        if (e) {
                            setPayloadAndSend(e);
                            this.raiseError(e);
                        }
                        else {
                            rm.status = Msg.STATUS_SUCCESS;
                            setPayloadAndSend(d);
                            debug(`Topic created ${msg.payload}`);
                        }
                    })
                }
                break;
            case Msg.ACTION_LIST:
                this.getTopicMetadata(ws.producer.client)
                    .then(data => {
                        rm.status = Msg.STATUS_SUCCESS;
                        setPayloadAndSend(data);
                    }).catch(e => {
                        setPayloadAndSend(e);
                        this.raiseError(e);
                });
                break;
            case Msg.ACTION_SUBSCRIBE:
                try {
                    this.initConsumerOrSubscribe(ws, msg);
                    debug(`Subscribing to topics: ${msg.payload}`);
                }catch (e) {
                    setPayloadAndSend(e);
                    throw e;
                }
                break;
            case Msg.ACTION_UNSUBSCRIBE:
                this.shutDownConsumer(ws);
                rm.status = Msg.STATUS_SUCCESS;
                ws.send(rm.toString());
                break;
        }
    }

    getTopicMetadata(client, topics){
        return new Promise((resolve, reject) => {
            client.loadMetadataForTopics(topics || [], (e, res) => {
                if (e){
                    return  reject(e);
                }else if (res && res.length > 1) {
                    let payload = [];

                    topics = topics || Object.keys(res[1].metadata).filter(n => !n.startsWith('__'));
                    topics.forEach(tn => {
                        if(res[1].metadata[tn]){
                            let partitions = Object.keys(res[1].metadata[tn]);
                            partitions.forEach(p => { payload.push({topic:tn, partition:parseInt(p, 10)}) })
                        }
                    });

                    return resolve(payload);
                }
                return reject({error: 'can not parse results', result: res});
            });
        });
    }

    initConsumerOrSubscribe(ws, msg) {

        const payload = msg.payload;

        let topicsPayload = [];
        if (Array.isArray(payload.t)) {
            topicsPayload = payload.t.map(t => {
                return {topic: t}
            });
        } else {

            let rm = Msg.createReplyMessage(msg);
            rm.status = Msg.STATUS_FAIL;
            rm.payload = "Topics should be an array";
            ws.send(rm.toString());
            return;
            // topicsPayload = payload.topics.map(t => {
            //     return {topic: t.topic, offset: t.offset || 0, partition: t.partition || 0}
            // });
        }

        this.shutDownConsumer(ws);

        const topic_names = topicsPayload.map(t=> t.topic);

        const client = new kafka.KafkaClient(Object.assign({}, this.kafka_config));



        //need to get metadata first to subscribe to each partition of the topic.
        this.getTopicMetadata(ws.producer.client, topic_names)
            .then(topics => {
                this.createConsumer(ws, client, topics, msg);
            })
            .catch(e => {
                const rm = Msg.createReplyMessage(msg);
                rm.status = Msg.STATUS_FAIL;
                rm.payload = e;
                ws.send(rm.toString());
                this.raiseError(e);
            });
    }

    createConsumer(ws, client, payload, msg) {

        const consumer_config_copy = Object.assign({}, this.consumer_config);
        consumer_config_copy.groupId = msg.payload.consumer_group || consumer_config_copy.groupId;

        debug(`Init consumer (consumer group: ${consumer_config_copy.groupId}) on: ${this.kafka_config.kafkaHost}`);

        ws.consumer = new kafka.Consumer(
            client,
            payload,
            consumer_config_copy);

        ws.consumer.response_msg = Msg.createReplyMessage(msg);

        ws.consumer.client.on('ready', function () {
            if (ws.consumer.response_msg) {
                if (ws.readyState === 1) {
                    let rm = ws.consumer.response_msg;
                    rm.status = Msg.STATUS_SUCCESS;
                    rm.payload = payload;
                    ws.send(rm.toString());
                }

                delete ws.consumer.response_msg;
            }

            this.emit('consumer-ready', ws.consumer);
            debug('Consumer is ready');
        });
        ws.consumer
            .on('error', e => {
                if (ws.consumer && ws.consumer.response_msg) {
                    let rm = ws.consumer.response_msg;
                    rm.status = Msg.STATUS_FAIL;
                    rm.payload = e;
                    ws.send(rm.toString());
                    delete ws.consumer.response_msg;
                }

                this.raiseError('consumer-error', e);
            })
            .on('offsetOutOfRange', e => {
                this.raiseError(e);
            })
            .on('message', function (data) {
                try {
                    this.emit('consumer-message', data)
                    ws.send(data.value);
                } catch (e) {
                    this.raiseError(e);
                }
            });
    }

    shutDownConsumer(ws){
        if (ws.consumer) {
            ws.consumer.close(WSKafkaConnector.edCallback);
            ws.consumer = null;
        }
    }
}

WebSocket.prototype.addToQueue = function (message) {
    this.mq.push(message);

    if (this.mq.length > 0) {
        if (this.mq.length >= this.mq_limit) {
            this.sendMessages();
        }
    }
};

WebSocket.prototype.shutDown = function () {

    try {
        debug(`Shutting down ws & kafka ${this.cnt}`);

        if (this.mq_send_interval)
            clearInterval(this.mq_send_interval);

        if (this.consumer) {
            this.consumer.close(WSKafkaConnector.edCallback);
            this.consumer = null;
        }

        if (this.producer) {
            this.producer.close(WSKafkaConnector.edCallback);
            this.producer = null;
        }
    } catch (e) {
        console.error(e);
    }
};

WebSocket.prototype.sendMessages = function () {

    try {
        if (this.mq.length === 0 || !this.producer.ready)
            return;

        let map = {};

        this.mq.forEach(msg => {
            let t = (map[msg.payload.t] = map[msg.payload.t] || {topic: msg.payload.t});
            (t.messages = t.messages || []).push(msg.payload.m);
        });

        this.mq.length = 0;

        let payload = Object.values(map);
        payload.attributes = 1; //GZIP
        //payload.partition = 0; //choose partition
        //payload.key = 'the key'; //key for keyed messages
        debug(payload);
        this.producer.send(payload, WSKafkaConnector.edCallback);

    } catch (e) {
        debug(e)
    }
};

module.exports.WSKafkaConnector = WSKafkaConnector;