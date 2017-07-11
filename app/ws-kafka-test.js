const WebSocket = require('ws');
const cfg   = require('./config-test');
const Msg   = require('./msg');

const ws = new WebSocket(process.env.WSS_URL || cfg.WSS_URL);

const args = process.argv.slice(2);

const PRODUCER = 'PRODUCER';
const CONSUMER = 'CONSUMER';

const type = args.length > 0 && args[0] === 'c'? CONSUMER : PRODUCER;
const MPS = 10000;
const TOTAL_MSGS = 100000;

process.on('uncaughtException', e => console.error(e));
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

ws.on('open', async function open() {
    try {
        if (type === PRODUCER) {

            var counter = 0;

            now = new Date().getTime();

            ws.send(
                Msg.createTopics([1,2,3,4,5]).toString()
            );

            wait_time = (now + 1000) - new Date().getTime();
            if (wait_time > 0)
                await sleep(wait_time).catch(e => console.error(e));

            while (counter < TOTAL_MSGS) {
                // console.log(`NOW:${new Date().getTime()} vs ${now + 1000}`);
                if (ws.readyState > 1) {
                    console.log(`WebSocket State ${ws.readyState}`)
                    return;
                }

                for (var i = 0; i < MPS; i++) {
                    counter++;
                    ws.send(
                        Msg.createNotification(
                            rand(1, 5),
                            `${Date.now()}`
                            //`Date: ${Date.now()} - '${Math.random().toString()}' - ${counter}`
                        ).toString()
                    );
                }
                console.log(`${counter} | ${new Date().getTime() - now} ms`);
                wait_time = (now + 1000) - new Date().getTime();
                if (wait_time > 0)
                    await sleep(wait_time).catch(e => console.error(e));

                now = new Date().getTime();
            }

            ws.close(1000);

        } else if (type === CONSUMER) {

            ws.send(Msg.createInfo().toString());

        }
    } catch (e) {
        console.error(e);
    }
})
.on('error', e => console.error(e))
.on('message', function incoming(data) {
    try {

        if (type === CONSUMER) {
            msg = Msg.fromJSON(data);

            if (msg.isInfo) {
                ws.send(Msg.createSubscribe(msg.payload).toString());
            }else{
                console.log(`#${++received}; travel time ${Date.now() - msg.payload} ms`);
            }
        }
    }
    catch (e) {
        console.error(e);
    }
});

var received = 0;

function rand(min,max){
    return Math.floor(Math.random()*(max-min+1)+min);
}
