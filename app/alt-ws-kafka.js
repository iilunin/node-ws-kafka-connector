const WebSocket = require('ws');
const kafka = require('kafka-node');
const cfg   = require('./config');
const Msg   = require('./msg');

// **************
// Init All
// **************
const wss = new WebSocket.Server({ port: process.env.WSS_PORT || cfg.WSS_PORT });

function getBrokerList(){
    return process.env.KAFKA_MBR || cfg.MBR_LIST;
}

function initProducer(ws){
    console.log(`Init producer on: ${getBrokerList()}`);

    const client = new kafka.KafkaClient(
        {
            'kafkaHost': getBrokerList(),
            'clientId':'test-kafka-client-2',
            'connectTimeout': 1000,
            'requestTimeout': 60000
        });

    ws.producer = new kafka.Producer(client, {
        'requireAcks': 1,
        'ackTimeoutMs': 100,
        'partitionerType': 2
    });

    ws.producer
        .on('ready', () => {
            resumeWsWhenKafkaReady(ws);
            console.log('Producer is ready')
        })
        .on('error', e => console.error(e));

    ws.mq = [];
    ws.mq_limit = 20000;
    ws.mq_send_interval = setInterval(ws.sendMessages.bind(ws), 1000);
}

WebSocket.prototype.addToQueue = function(message){
    this.mq.push(message);

    if(this.mq.length > 0){
        if(this.mq.length >= this.mq_limit){
            this.sendMessages();
        }
        //Schedule to send
    }
}

WebSocket.prototype.shutDown = function() {

    try{
        console.log(`Shutting down ws & kafka ${this.cnt}`);

        clearInterval(this.mq_send_interval);
        if(this.consumer && this.consumer.ready){
            this.consumer.close(edCallback);
            this.consumer = null;

        }
        if(this.producer && this.producer.ready){
            this.producer.close(edCallback);
            this.producer = null;
        }
    }catch(e){
        console.error(e);
    }
}

WebSocket.prototype.sendMessages = function(){

    try {
        if (this.mq.length == 0 || !this.producer.ready)
            return;

        let map = {};

        this.mq.forEach(msg => {
            t = (map[msg.device_id] = map[msg.device_id] || {topic: msg.device_id});
            (t.messages = t.messages || []).push(msg.toString());
        });

        this.mq.length = 0;
        this.producer.send(Object.values(map), edCallback);

    } catch (e) {
        console.log(e)
    }
}

// **************
// Kafka Consumer
// **************
function initConsumer(ws, topics){
    console.log(`Init consumer on: ${getBrokerList()}`);
    const client = new kafka.KafkaClient(
        {
            'kafkaHost': getBrokerList(),
            'clientId':'test-kafka-client-3',
            'connectTimeout': 1000
        });

    topicsPayload = topics.map(t => {
        return {topic: t, partition: 0}
    });

    ws.consumer = new kafka.Consumer(client, topicsPayload, {
            groupId: 'kafka-node-group',
    // Auto commit config
            autoCommit: true,
            autoCommitMsgCount: 100,
            autoCommitIntervalMs: 5000,
    // Fetch message config
            fetchMaxWaitMs: 100,
            fetchMinBytes: 1,
            fetchMaxBytes: 1024 * 10,
        }
    );

    // const offset = new kafka.Offset(client);

    ws.consumer
        .on('ready', function () {
            try {
                console.log('Consumer is ready');
//                resumeWsWhenKafkaReady(ws);
            }
            catch (e) {
                console.error(e);
            }
        })
        .on('error', e => console.error(e))
        .on('offsetOutOfRange', e => {
            console.error(e);
        })
        .on('message', function (data) {
            try {
                // console.log(`Message received from Kafka ${data}`);
                ws.send(data.value);
            } catch (e) {
                console.error(e);
            }
        })
        .on('disconnected', () => console.log(`Consumer of ws ${ws.cnt} has disconnected`));

//    ws.consumer.connect();
}

function resumeWsWhenKafkaReady(ws){
    if(ws.producer.ready){
        ws.resume();
        console.log(`Resuming ws ${ws.cnt}`);
    }
}

function edCallback(e, d){
    if(e)console.error(e);
    if(d)console.log(d);
}

// **************
// WebSocket Receive
// **************

ws_cnt = 0;
wss.on('connection', ws => {
    try {
        ws.pause();
        // initConsumer(ws);
        initProducer(ws);

        ws.cnt = ++ws_cnt;
        ws.on('close', () => {
            ws.shutDown();
        });
        ws.on('pong', () => {
            try {
                ws.isAlive = true;
                console.log(`Pong received from ws ${ws.cnt}`);
            } catch (e) {
                console.error(e);
            }
        }); //heartbeat
        wsOnMsg(ws);
    } catch (e) {
        console.error(e);
        ws.resume();
    }
});

now = new Date().getTime();

function wsOnMsg(ws){
    ws.on('message', msg => {
        console.log(msg);
        try {
            let m = Msg.fromJSON(msg);
            if (m.isCreateTopic){
                ws.producer.createTopics(m.payload, false, (e,d) =>{
                    if(e) console.error(e);
                    else
                        console.log(`Created topics ${m.payload}`);
                })

            }
            if (m.isInfo) { //Get info about kafka topics
                // console.log(ws.producer);
                ws.producer.client.loadMetadataForTopics([], (err, res) => {
                    if(err) console.error(err);

                    if(res && res.length > 1){
                        m.payload = Object.keys(res[1].metadata).filter(n => !n.startsWith('__'));
                        ws.send(m.toString());
                        console.log(m.toString());
                    }
                });
            }
            else if (m.isSubscribe) { //Subscribe to specific kafka topics
                initConsumer(ws, m.payload);
                // ws.consumer.addTopics(m.payload, cb, fromOffset);
                // ws.consumer.unsubscribe();
                // ws.consumer.subscribe(m.payload);

                console.log(`Subscribed to topics: ${m.payload}`);
            }
            else if (m.isNotification) { //publish notificaotin to kafka
                // ws.producer.produce(
                //     m.device_id,
                //     null,
                //     new Buffer(msg),
                //     null,
                //     Date.now()
                // );
                ws.addToQueue(m);
                // ws.sendMessages([msg]);

                // if(new Date().getTime() - now > 100) {
                //     ws.producer.flush();
                //     now = new Date().getTime();
                // }
            }
        }
        catch (e) {
            if (e instanceof SyntaxError) {
                console.error(`Invalid JSON string '${msg}'`);
            } else {
                console.error(e);
            }
        }
    });
}


// **************
// WebSocket ping
// **************
const interval = setInterval(function ping() {
    wss.clients.forEach(ws => {
        if (ws.isAlive === false) return ws.terminate();

        ws.isAlive = false;
        console.log(`Pinging ws ${ws.cnt}`);
        ws.ping('', false, true);
    });
}, 60000);


// **************
// Exit handler
// **************

function exitHandler(obj){
    if(obj.err) console.error(obj.err);
    clearInterval(interval);
    if(wss) wss.close();
    console.log("wss closed");
}

//do something when app is closing
process
    // .on('exit', exitHandler.bind(null, {cleanup: true}))
    .on('SIGINT', exitHandler.bind(null, {exit: true}))
    .on('uncaughtException', exitHandler.bind(null, {exit: true}));
