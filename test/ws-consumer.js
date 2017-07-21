const WebSocket = require('ws'),
    cfg = require('./config-test'),
    debug = require('debug')('ws-consumer');


const TOPIC_COUNT = cfg.TOPICS_COUNT || 5;

process.on('uncaughtException', e => console.error(e))
    .on('SIGINT', ()=>{
        clearInterval(mrate);
        ws.close()});

const ws = new WebSocket(process.env.WSS_URL || cfg.WSS_URL);

let counter = 0;
let last_counter = 0;

const mrate = setInterval(function () {
    let rate = counter - last_counter;
    if(rate > 0) {
        console.log(`${rate} msgs/s`);
        last_counter = counter;
    }
}, 1000);

ws.on('open', () => {
    subscribeTopics(ws);
}).on('message', (data) => {
    let msg = JSON.parse(data);
    if(msg.refid === "0000"){
       console.log(msg);
    }else{
        counter++;
        if(counter % 1000 === 0){
            let lag = -1;
            if(msg.p){
                lag = new Date().getTime() - msg.p;
            }

            console.log(`${counter} messages received with interval ${lag}`);
        }


        debug(msg);
    }
});

function subscribeTopics(ws) {
    let msg = {
        id:"0000",
        t:"topic",
        a:"subscribe",
        p:{"t":[],consumer_group:"ingestion_1"}
    };

    for(let i = 0; i < TOPIC_COUNT; i++){
        msg.p.t.push(`topic_${i}`);
    }

    ws.send(JSON.stringify(msg));
}