const kafka = require('kafka-node');
const cfg   = require('./config');


const client = new kafka.KafkaClient(
    {
        'kafkaHost': process.env.KAFKA_MBR || cfg.MBR_LIST,
        'clientId':'test-kafka-client-2',
        'connectTimeout': 1000
    });

const producer = new kafka.Producer(client, {
    'requireAcks': 1,
    'ackTimeoutMs': 100,
    'partitionerType': 2
});



const args = process.argv.slice(2);
MSG_NUM = 1;
MSG_CONFIRMED = 0;

producer.on('ready', pushMessages);
producer.on('error', e => console.error(e));
// producer.on('delivery-report', function (err, report) {
//     console.log("delivery report");
//
//     if (++MSG_CONFIRMED == MSG_NUM) {
//         producer.disconnect();
//     }
// });

// producer.connect(null, (e, d) =>{
//     if(e)console.error(e);
//     if(d)console.log(d);
// });

// producer.setPollInterval(100);

function pushMessages(){
    try {

        producer.createTopics(['1', '2'], false, edCallback);

        if(args.length > 0){
            MSG_NUM = parseInt(args[0]);
        }
        for (i = 0; i < MSG_NUM; i++) {
            console.log(i);
            producer.send(
                [
                    {topic: "1", messages: ['Test message ' + Math.random().toString()]},
                    {topic: "2", messages: ['Test message ' + Math.random().toString()]}
                ],
                (e, d) => {
                    if (++MSG_CONFIRMED == MSG_NUM){
                        console.log(d);
                        producer.close();
                    }
                }
            );
        }


        // producer.flush(5000, args => console.log(args));
    }catch(e){
        console.error(e);
        // producer.disconnect((e,d) => {
        //     if(e) console.error(e);
        //     if(d) console.log(d);
        // })
    }
}

function edCallback(e, d){
    if(e) console.error(e);
    if(d) console.log(d);
}


// pushMessages();


function exitHandler(options, err) {
    // if (options.cleanup) console.log('clean');
    // if (err) console.log(err.stack);
    // if (options.exit) process.exit();
    if(producer && producer.ready) producer.close();
}

//do something when app is closing
process.on('exit', exitHandler.bind(null, {cleanup: true}));

//catches ctrl+c event
process.on('SIGINT', exitHandler.bind(null, {exit: true}));

//catches uncaught exceptions
process.on('uncaughtException', exitHandler.bind(null, {exit: true}));
