FROM flink:1.20.3-java17
USER flink
RUN mkdir -p /opt/flink/jars/flink-web-upload
COPY --chown=flink:flink target/etl-template-with-flink*.jar /opt/flink/jars/flink-web-upload/etl-template-with-flink.jar
ENV FLINK_PROPERTIES="web.upload.dir: /opt/flink/jars"