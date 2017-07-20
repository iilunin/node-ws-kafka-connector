FROM node:8.1.4-alpine
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
        kafka-node

#RUN npm install pm2 -g

COPY . ${WORK_DIR}

RUN npm install \
    && apk del .gyp

EXPOSE 8080
CMD ["node", "test/wskafka-server.js"]
#CMD ["pm2-docker", "alt-ws-kafka.js"]
