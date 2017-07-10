FROM node:8.1.2
ENV WITH_SASL 0
ENV WORK_DIR=/usr/src/app/
RUN mkdir -p ${WORK_DIR} \
  && cd ${WORK_DIR}

#RUN apk add --no-cache --virtual .gyp \
#      python \
#      make \
#      g++

WORKDIR ${WORK_DIR}
COPY app ${WORK_DIR}

RUN npm install \
    ws \
    node-rdkafka

RUN npm install \
    && npm install pm2 -g
    # \
#    && apk del make gcc g++ python

EXPOSE 8080
CMD ["node", "ws-kafka-producer-consumer.js"]
#CMD ["pm2-docker", "ws-kafka-producer-consumer.js"]
