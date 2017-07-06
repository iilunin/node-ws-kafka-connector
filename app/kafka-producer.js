const kafka = require('node-rdkafka');
const cfg   = require('./config');

const args = process.argv.slice(2);
MSG_NUM = 1;
MSG_CONFIRMED = 0;

const producer = new kafka.Producer({
    'metadata.broker.list': cfg.MBR_LIST,
    'dr_cb': true,
    'client.id': 'my-client'
});

producer.connect();
producer.setPollInterval(100);

function pushMessages(){
  if(args.length > 0){
    MSG_NUM = parseInt(args[0]);
  }

  for(i = 0; i < MSG_NUM; i++){
    console.log(i);
    produce();
  }
}

function produce(){
  return producer.produce(
    cfg.KAFKA_TOPIC,
    null,
    new Buffer('Test message ' + Math.random().toString()),
    undefined,
    Date.now()
  )
}

// pushMessages();
producer.on('ready', pushMessages);
producer.on('delivery-report', function(err, report) {
  if (++MSG_CONFIRMED  == MSG_NUM){
    producer.disconnect();
  }
});

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
