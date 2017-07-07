const WebSocket = require('ws');
const cfg   = require('./config-test');
const Msg   = require('./msg');

const ws = new WebSocket(cfg.WSS_URL);

const PRODUCER = 'PRODUCER';
const CONSUMER = 'CONSUMER';

const type = PRODUCER;
const MPS = 5000;
const TOTAL_MSGS = 1000000;




ws.on('open', function open() {
  if(type === PRODUCER){

    var counter = 0;

    now = new Date().getTime();

    while(counter < TOTAL_MSGS){
      console.log(`NOW:${new Date().getTime()} vs ${now + 1000}`);

      for(var i = 0; i < MPS; i++){
        counter++;
        ws.send(
          Msg.createNotification(
            rand(1, 5),
            `Date: ${Date.now()} - '${Math.random().toString()}'`
          ).toString()
        );

        console.log(counter);
      }
      while((now + 1000) > new Date().getTime()){
      }

      now = new Date().getTime();
    }

    ws.close(1000);

  }else if (type === CONSUMER){

  }
}).on('error', e => console.error(e));

ws.on('message', function incoming(data) {
  console.log(data);
});

function rand(min,max){
    return Math.floor(Math.random()*(max-min+1)+min);
}
