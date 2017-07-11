const kafka = require('node-rdkafka');
const cfg   = require('./config');

const args = process.argv.slice(2);
MSG_NUM = 1;
MSG_CONFIRMED = 0;

const producer = new kafka.Producer({
    'metadata.broker.list': process.env.KAFKA_MBR || cfg.MBR_LIST,
    'dr_cb': true,
    'dr_msg_cb': true,
    'client.id': 'test-kafka-client'
}, {
    'request.required.acks': 1
});

producer.on('ready', pushMessages);
producer.on('error', e => console.error(e));
producer.on('delivery-report', function (err, report) {
    console.log("delivery report");

    if (++MSG_CONFIRMED == MSG_NUM) {
        producer.disconnect();
    }
});

producer.connect(null, (e, d) =>{
    if(e)console.error(e);
    if(d)console.log(d);
});

producer.setPollInterval(100);

function pushMessages(){
  if(args.length > 0){
    MSG_NUM = parseInt(args[0]);
  }

  try {
      for (i = 0; i < MSG_NUM; i++) {
          console.log(i);
          console.log(produce())
      }

      // producer.flush(5000, args => console.log(args));
  }catch(e){
      console.error(e);
      producer.disconnect((e,d) => {
          if(e) console.error(e);
          if(d) console.log(d);
      })
  }
}

function produce(){
  return producer.produce(
    "1",
    null,
    new Buffer('Test message ' + Math.random().toString()),
    undefined,
    Date.now()
  )
}

// pushMessages();


function exitHandler(options, err) {
  // if (options.cleanup) console.log('clean');
  // if (err) console.log(err.stack);
  // if (options.exit) process.exit();
  if(producer) producer.disconnect();
}

//do something when app is closing
process.on('exit', exitHandler.bind(null, {cleanup: true}));

//catches ctrl+c event
process.on('SIGINT', exitHandler.bind(null, {exit: true}));

//catches uncaught exceptions
process.on('uncaughtException', exitHandler.bind(null, {exit: true}));
