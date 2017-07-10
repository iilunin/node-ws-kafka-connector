FROM node
ENV WORK_DIR=/usr/src/app/
RUN mkdir -p ${WORK_DIR} \
  && cd ${WORK_DIR}
WORKDIR ${WORK_DIR}
COPY app ${WORK_DIR}
RUN npm install
RUN npm install pm2 -g

EXPOSE 8080
#CMD ["node", "ws-kafka-producer-consumer.js"]
CMD ["pm2-docker", "ws-kafka-producer-consumer.js"]
