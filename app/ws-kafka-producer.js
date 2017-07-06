const WebSocket = require('ws');
const kafka = require('node-rdkafka');
const cfg   = require('./config');


// **************
// Kafka Producer
// **************
const producer = new kafka.Producer({
    'metadata.broker.list': cfg.MBR_LIST,
    'dr_cb': true,
    'client.id': 'my-client'
});

producer.connect();

function exitHandler(options, err) {
  // if (options.cleanup) console.log('clean');
  // if (err) console.log(err.stack);
  // if (options.exit) process.exit();
  if(producer) producer.disconnect();
}

//do something when app is closing
process.on('exit', exitHandler.bind(null,{cleanup:true}));

//catches ctrl+c event
process.on('SIGINT', exitHandler.bind(null, {exit:true}));

//catches uncaught exceptions
process.on('uncaughtException', exitHandler.bind(null, {exit:true}));



// **************
// WebSocket
// **************
const wss = new WebSocket.Server({ port: 8080 });

function heartbeat() {
  this.isAlive = true;
}

wss.on('connection', ws => {
  ws.on('pong', () => ws.isAlive = true);

  ws.on('message', msg => {
    ws.send(`echo '${msg}'`);
    producer.produce(
      cfg.KAFKA_TOPIC,
      null,
      new Buffer(msg),
      undefined,
      Date.now()
    )

  });
});

const interval = setInterval(function ping() {
  wss.clients.forEach(ws => {
    if (ws.isAlive === false) return ws.terminate();

    ws.isAlive = false;
    ws.ping('', false, true);
  });
}, 30000);
