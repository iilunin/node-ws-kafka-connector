const kafka = require('node-rdkafka');
const cfg   = require('./config');

const consumer = new kafka.KafkaConsumer({
    'metadata.broker.list': cfg.MBR_LIST,
    'group.id': 'my_group_id',
    'fetch.wait.max.ms': 1,
    'fetch.min.bytes': 1,
    'queue.buffering.max.ms': 1,
    'enable.auto.commit': true
});

consumer.connect();

consumer.on('ready', function() {

    consumer.subscribe([cfg.KAFKA_TOPIC]);
    consumer.consume();
  })
  .on('data', function(data) {
    console.log(`Got a message: \n` +
                `   topic: ${data.topic}\n` +
                `   partition: ${data.partition}\n` +
                `   offset: ${data.offset}\n` +
                `   payload: ${data.value.toString()}\n`);
});

function exitHandler(options, err) {
  // if (options.cleanup) console.log('clean');
  // if (err) console.log(err.stack);
  // if (options.exit) process.exit();
  if(consumer) consumer.disconnect();
}

//do something when app is closing
process.on('exit', exitHandler.bind(null,{cleanup:true}));

//catches ctrl+c event
process.on('SIGINT', exitHandler.bind(null, {exit:true}));

//catches uncaught exceptions
process.on('uncaughtException', exitHandler.bind(null, {exit:true}));
