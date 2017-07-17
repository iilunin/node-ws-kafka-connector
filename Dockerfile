#FROM node:8.1.2
FROM node:8.1.4-alpine
ENV WITH_SASL 0
ENV WORK_DIR=/usr/src/app/
RUN mkdir -p ${WORK_DIR} \
  && cd ${WORK_DIR}

WORKDIR ${WORK_DIR}

RUN apk add --no-cache --virtual .gyp \
        python \
        make \
        g++ \
        && npm install \
        ws \
#    node-rdkafka \
        kafka-node

#RUN npm install pm2 -g

COPY app ${WORK_DIR}

RUN npm install \
    && apk del .gyp

EXPOSE 8080
CMD ["node", "alt-ws-kafka.js"]
#CMD ["pm2-docker", "alt-ws-kafka.js"]
