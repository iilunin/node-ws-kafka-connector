const WebSocket = require('ws');
const kafka = require('kafka-node');
const cfg   = require('./config');
const Msg   = require('./msg');

// **************
// Init All
// **************
const wss = new WebSocket.Server({ port: process.env.WSS_PORT || cfg.WSS_PORT });

function getBrockerList(){
    return process.env.KAFKA_MBR || cfg.MBR_LIST;
}

function initProducer(ws){
    const client = new kafka.KafkaClient(
        {
            'kafkaHost': getBrockerList(),
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
}

// **************
// Kafka Consumer
// **************
function initConsumer(ws, topics){
    const client = new kafka.KafkaClient(
        {
            'kafkaHost': getBrockerList(),
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
                console.log(`Message received from Kafka ${data}`);
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

function shutDownKafka(ws){
    try{
        console.log(`Shutting down kafka ws ${ws.cnt}`);
        if(ws.consumer && ws.consumer.ready){
            ws.consumer.close(edCallback);
            ws.consumer = null;

        }
        if(ws.producer && ws.producer.ready){
            ws.producer.close(edCallback);
            ws.producer = null;
        }
    }catch(e){
        console.error(e);
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
        ws.on('close', () => shutDownKafka(ws));
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

                ws.producer.send(
                    [
                        {topic: m.device_id, messages: [msg.toString()]}
                    ],
                    edCallback
                );
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
