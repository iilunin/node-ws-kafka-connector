const WebSocket = require('ws');
const kafka = require('node-rdkafka');
const cfg   = require('./config');
const Msg   = require('./msg');

// **************
// Init All
// **************
const wss = new WebSocket.Server({ port: process.env.WSS_PORT || cfg.WSS_PORT });

const producer = new kafka.Producer({
    'metadata.broker.list': cfg.MBR_LIST,
    'queue.buffering.max.messages': 1000000,
    'queue.buffering.max.ms': 20000,
    'dr_cb': true,
    'client.id': 'my-client'
});

const consumer = new kafka.KafkaConsumer({
    'metadata.broker.list': cfg.MBR_LIST,
    'group.id': 'web_socket_group',
    'fetch.wait.max.ms': 1,
    'fetch.min.bytes': 1,
    'queue.buffering.max.ms': 100,
    'enable.auto.commit': true
});

consumer.connect();
producer.connect();

// **************
// Kafka Consumer
// **************
producer.on('ready', () => console.log('Producer is ready'))
  .on('error',console.error);

consumer.on('ready', function() {
    console.log('Consumer is ready');
  })
  .on('data', function(data) {
    console.log(data);
    s_msg = data.value.toString();
    console.log(`Message received from Kafka ${s_msg}`)
    try{

      msg = Msg.fromJSON(s_msg);

      wss.clients.forEach(ws => {
        if (ws.isAlive === false) return ws.terminate();
        ws.send(s_msg);
      });
    }catch(e){
      console.error(e);
    }
});

// **************
// WebSocket Receive
// **************
wss.on('connection', ws => {
  ws.on('pong', () => ws.isAlive = true); //heartbeat

  ws.on('message', msg => {
    console.log(msg);

    try{
      m = Msg.fromJSON(msg);

      if (m.isInfo){ //Get info about kafka topics
        producer.getMetadata(null, (err, res) => {
          if(err){
            console.error(err);
            return;
          }
          topics = res.topics.map(t => t.name).filter(n => !n.startsWith('__'));
          m.payload = topics;
          ws.send(m.toString());
        });
      }
      else if(m.isSubscribe){ //Subscribe to specific kafka topics
        consumer.unsubscribe();
        consumer.subscribe([m.payload]);
        consumer.consume();

        console.log(`Filter for ws set to ${m.payload}`);
      }
      else if (m.isNotification){ //publish notificaotin to kafka
        producer.produce(
          m.device_id,
          null,
          new Buffer(msg),
          undefined,
          Date.now()
        )
        console.log(`Message sent to kafka`);
      }
    }
    catch(e){
      if(e instanceof SyntaxError){
        console.error(`Invalid JSON string '${msg}'`);
      }else{
        console.error(e);
      }
    }
  });
});


// **************
// WebSocket ping
// **************
const interval = setInterval(function ping() {
  console.log('ping');
  wss.clients.forEach(ws => {
    if (ws.isAlive === false) return ws.terminate();

    ws.isAlive = false;
    ws.ping('', false, true);
  });
}, 30000);


// **************
// Exit handler
// **************

function exitHandler(options, err) {
  clearInterval(interval);
  if(producer) producer.disconnect();
  if(consumer) consumer.disconnect();
}

//do something when app is closing
process.on('exit', exitHandler.bind(null,{cleanup:true}));

//catches ctrl+c event
process.on('SIGINT', exitHandler.bind(null, {exit:true}));

//catches uncaught exceptions
process.on('uncaughtException', exitHandler.bind(null, {exit:true}));
