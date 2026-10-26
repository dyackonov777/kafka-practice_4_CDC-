FROM debezium/connect:2.6

ARG JMX_EXPORTER_VERSION=0.20.0

# Java-агент, который публикует JMX-метрики Kafka Connect и Debezium в формате Prometheus
RUN mkdir -p /kafka/jmx && \
    curl -sSfL -o /kafka/jmx/jmx_prometheus_javaagent.jar \
    https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${JMX_EXPORTER_VERSION}/jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar
