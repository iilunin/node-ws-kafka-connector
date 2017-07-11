FROM node:8.1.2
ENV WITH_SASL 0
ENV WORK_DIR=/usr/src/app/
RUN mkdir -p ${WORK_DIR} \
  && cd ${WORK_DIR}

WORKDIR ${WORK_DIR}

RUN npm install \
    ws \
#    node-rdkafka \
    kafka-node

RUN npm install pm2 -g

COPY app ${WORK_DIR}

RUN npm install

EXPOSE 8080
#CMD ["node", "ws-kafka-producer-consumer.js"]
CMD ["pm2-docker", "alt-ws-kafka.js"]
