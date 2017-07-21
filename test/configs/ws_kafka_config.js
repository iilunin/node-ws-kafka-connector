function getBrokerList(){
    return process.env.KAFKA_MBR || 'kafka:9094';
}

function getWebSocketPort(){
    return process.env.WSS_PORT || '8080';
}

module.exports.kafka_config = {
    //node-kafka options
    kafkaHost: getBrokerList(),
    clientId: 'test-kafka-client-2',
    connectTimeout: 1000,
    requestTimeout: 60000,
    autoConnect: true,
    //custom options
    no_zookeeper_client: true,
    mq_limit: 20000,
    mq_interval: 200 //if null, then messages published immediately
}

module.exports.websocket_config ={
    port: getWebSocketPort()
}

module.exports.producer_config = {
    requireAcks: 1,
    ackTimeoutMs: 100,
    partitionerType: 2
}

module.exports.consumer_config ={
    groupId: 'kafka-node-group', //should be set by message to ws
    // Auto commit config
    autoCommit: true,
    autoCommitMsgCount: 100,
    autoCommitIntervalMs: 5000,
    // Fetch message config
    fetchMaxWaitMs: 100,
    fetchMinBytes: 1,
    fetchMaxBytes: 1024 * 1024,
    // Offset
    fromOffset: false
}
