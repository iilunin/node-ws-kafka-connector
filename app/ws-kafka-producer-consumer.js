const WebSocket = require('ws');
const kafka = require('node-rdkafka');
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
  ws.producer = new kafka.Producer({
      'metadata.broker.list': getBrockerList(),
      'queue.buffering.max.messages': 10000,
      'queue.buffering.max.ms': 100,
      'batch.num.messages': 100,
      'dr_cb': true,
      'client.id': 'my-client'
  });

  ws.producer.connect();

  ws.producer_ready = false;
  ws.producer
    .on('ready', () => {
        ws.producer_ready = true;
        resumeWsWhenKafkaReady(ws);
        console.log('Producer is ready')
    })
    .on('error', e => console.error(e))
    .on('disconnected', () => console.log(`Producer of ws ${ws.cnt} has disconnected`));
}

// **************
// Kafka Consumer
// **************
function initConsumer(ws){
  ws.consumer = new kafka.KafkaConsumer({
      'metadata.broker.list': getBrockerList(),
      'group.id': 'web_socket_group',
      'fetch.wait.max.ms': 1,
      'fetch.min.bytes': 1,
      'queue.buffering.max.ms': 100,
      'enable.auto.commit': true
  });

  ws.consumer_ready = false;
  ws.consumer.on('ready', function () {
    try {
        console.log('Consumer is ready');
        ws.consumer_ready = true;
        resumeWsWhenKafkaReady(ws);
        ws.consumer.consume(null);
    }
    catch (e) {
        console.error(e);
    }
  })
    .on('error', e => console.error(e))
    .on('data', function (data) {
        try {
        console.log(`Message received from Kafka ${data}`);
        ws.send(data.value.toString());
        } catch (e) {
        console.error(e);
        }
    })
    .on('disconnected', () => console.log(`Consumer of ws ${ws.cnt} has disconnected`));

  ws.consumer.connect();
}

function resumeWsWhenKafkaReady(ws){
   if(ws.consumer_ready && ws.producer_ready){
     ws.resume();
     console.log(`Resuming ws ${ws.cnt}`);
   }
}

function shutDownKafka(ws){
  try{
    console.log(`Shutting down kafka ws ${ws.cnt}`);
    if(ws.consumer){
      ws.consumer.unsubscribe();
      ws.consumer.disconnect();
      ws.consumer = null;
    }
    if(ws.producer){
      // ws.producer.flush();
      ws.producer.disconnect();
      ws.producer = null;
    }
  }catch(e){
    console.error(e);
  }
}

// **************
// WebSocket Receive
// **************

ws_cnt = 0;
wss.on('connection', ws => {
    ws.pause();
    initConsumer(ws);
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
});

function wsOnMsg(ws){
  ws.on('message', msg => {
    console.log(msg);
      try {
          let m = Msg.fromJSON(msg);

          if (m.isInfo) { //Get info about kafka topics
              ws.producer.getMetadata(null, (err, res) => {
                  if (err) {
                      console.error(err);
                      return;
                  }
                  let topics = res.topics.map(t => t.name).filter(n => !n.startsWith('__'));
                  m.payload = topics;
                  ws.send(m.toString());
                  console.log(m.toString());
              });
          }
          else if (m.isSubscribe) { //Subscribe to specific kafka topics
              ws.consumer.unsubscribe();
              ws.consumer.subscribe(m.payload);

              console.log(`Subscribed to topics: ${m.payload}`);
          }
          else if (m.isNotification) { //publish notificaotin to kafka
              ws.producer.produce(
                  m.device_id,
                  null,
                  new Buffer(msg),
                  null,
                  Date.now()
              )
              // console.log(`Message sent to kafka`);
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
}

//do something when app is closing
process
  .on('exit', exitHandler.bind(null, {cleanup: true}))
  .on('SIGINT', exitHandler.bind(null, {exit: true}))
  .on('uncaughtException', exitHandler.bind(null, {exit: true}));
