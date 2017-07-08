const MSG_TYPE_INFO = 'info';
const MSG_TYPE_SUBSCRIBE = 'subscribe';
const MSG_TYPE_NOTIFY     = 'notification';

module.exports = class Msg {
  constructor(type, device_id, payload) {
    this.type = type;
    this.device_id = device_id;
    this.payload = payload;
  }

  toString(){
    if(this.isSubscribe){
      return `{"type":"${MSG_TYPE_SUBSCRIBE}"` + (this.payload? `,"payload":${JSON.stringify(this.payload)}`:``) + `}`;
    }
    if(this.isInfo){
      // return `{"type":"${MSG_TYPE_INFO}","payload":${JSON.stringify(this.payload)}}`;
      return `{"type":"${MSG_TYPE_INFO}"` + (this.payload? `,"payload":${JSON.stringify(this.payload)}`:``) + `}`;
    }

    return `{"type":"${this.type}","device_id":"${this.device_id}","payload":"${this.payload}"}`;
  }

  get isInfo(){
    return this.type == MSG_TYPE_INFO;
  }

  get isSubscribe(){
    return this.type == MSG_TYPE_SUBSCRIBE;
  }

  get isNotification(){
    return this.type == MSG_TYPE_NOTIFY;
  }

  static get MSG_TYPE_NOTIFY() {
    return MSG_TYPE_NOTIFY;
  }

  static get MSG_TYPE_SUBSCRIBE() {
    return MSG_TYPE_SUBSCRIBE;
  }

  static fromJSON(json){
    var obj = JSON.parse(json);
    return new Msg(obj.type, obj.device_id, obj.payload);
  }

  static createNotification(device_id, payload){
    return new Msg(MSG_TYPE_NOTIFY, device_id, payload);
  }

  static createSubscribe(devices){
    return new Msg(MSG_TYPE_SUBSCRIBE, null, devices);
  }

  static createInfo(){
    return new Msg(MSG_TYPE_INFO, null, null);
  }
}
